//! [`Server`]: an accept loop that serves one [`Endpoint`] per connection,
//! over TCP or (on Unix) a Unix domain socket. The `net` feature.

use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    config::Config,
    endpoint::{End, Endpoint, Next},
    error::Error,
    framing::Framing,
    queue::{QueueLimits, Queued},
    router::Router,
    trace::event,
};

/// How long the stopper waits to wake a blocked `accept` by connecting.
const WAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// On Windows, how often a waiting reader looks for a stop and its
/// deadline (see [`Stream::wait_readable`]).
#[cfg(windows)]
const TICK: Duration = Duration::from_millis(250);

/// The first and the longest pause of the accept loop when the system is
/// out of descriptors or memory.
const BACKOFF: (Duration, Duration) = (Duration::from_millis(10), Duration::from_secs(1));

/// One accepted connection, as the server's `connect` callback sees it.
///
/// # Examples
///
/// See [`Server::serve_tcp`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Connection {
    /// The server's number for it, counted from 0.
    pub id: u64,
    /// The peer's address: `ip:port` for TCP, the socket path (or empty) for
    /// a Unix domain socket.
    pub addr: String,
}

/// An accept loop: one [`Endpoint`] per connection, each on its own thread,
/// with output through a [`Queued`] sink.
///
/// | Setting | Default | Meaning |
/// |---|---|---|
/// | `framing` | (given) | the framing of every connection |
/// | `config` | [`Config::new`] | every endpoint's settings |
/// | `queue` | [`QueueLimits::new`] | every connection's outgoing queue |
/// | `max_connections` | 256 | connections served at once; the next is not accepted until one ends |
/// | `message_timeout` | 30 s | how long a message may take to arrive once it has begun, and a new connection to send its first one |
/// | `idle_timeout` | 300 s | how long a connection may stay silent between messages |
///
/// Concurrency is bounded: at `max_connections`, the loop stops accepting
/// (the operating system queues further connections in its backlog) until a
/// connection ends. So is the time a connection can hold a slot without
/// doing anything: a connection that does not complete a message within
/// `message_timeout` of starting it (or of connecting), or sends nothing for
/// `idle_timeout` between messages, is closed. A few silent or trickling
/// sockets therefore delay other clients by at most `message_timeout`; they
/// cannot lock them out. Each connection costs a reader thread, a writer
/// thread, and, once a request with a deadline is sent on it, a timer thread.
///
/// The writer's socket gets the queue's `write_timeout` as its write timeout,
/// so a peer that stops reading fails the write itself, and a failed write
/// shuts the connection down. On Windows, where shutting a socket down does
/// not wake a read blocked on it and a timed-out receive leaves the socket in
/// an undefined state, the reader waits for data with a timed `peek` (which
/// consumes nothing, so its timeout loses nothing), reads only once data is
/// there, and closes the connection when a deadline passes.
///
/// [`stopper`](Server::stopper) gives a handle that ends the loop from any
/// thread: it stops accepting, shuts every open connection down (their peers
/// see the stream end), and the serve call returns once every connection's
/// thread has finished.
///
/// # Examples
///
/// ```
/// use std::{io::{BufRead, BufReader, Write}, net::{TcpListener, TcpStream}};
/// use rpc_io::{Framing, Router, Server};
///
/// let listener = TcpListener::bind("127.0.0.1:0").map_err(rpc_io::Error::Io)?;
/// let addr = listener.local_addr().map_err(rpc_io::Error::Io)?;
/// let server = Server::new(Framing::Newline);
/// let stopper = server.stopper();
/// let serving = std::thread::spawn(move || {
///     server.serve_tcp(listener, |_connection| {
///         (Router::new().request("ping", |_: &mut (), _, _| Ok("pong")), ())
///     })
/// });
///
/// let mut client = TcpStream::connect(addr).map_err(rpc_io::Error::Io)?;
/// client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n").map_err(rpc_io::Error::Io)?;
/// let mut line = String::new();
/// BufReader::new(&client).read_line(&mut line).map_err(rpc_io::Error::Io)?;
/// assert_eq!(line, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
///
/// stopper.stop();
/// assert!(serving.join().is_ok_and(|served| served.is_ok()));
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Server {
    framing: Framing,
    config: Config,
    queue: QueueLimits,
    max_connections: usize,
    deadlines: Deadlines,
    shared: Arc<Shared>,
}

/// How long a connection may take to send a message, or stay silent.
#[derive(Clone, Copy, Debug)]
struct Deadlines {
    message: Option<Duration>,
    idle: Option<Duration>,
}

/// What the server and its stoppers share.
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    /// Signals a connection that ended, or a stop.
    changed: Condvar,
}

#[derive(Default)]
struct State {
    stopped: bool,
    /// Connections being served.
    active: usize,
    /// Wakes the accept loop by connecting to the listener.
    wake: Option<Box<dyn Fn() + Send>>,
    /// Shuts each open connection down.
    open: BTreeMap<u64, Box<dyn Fn() + Send>>,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn stop(&self) {
        let (wake, open) = {
            let mut state = self.lock();
            state.stopped = true;
            (state.wake.take(), std::mem::take(&mut state.open))
        };
        self.changed.notify_all();
        for shut in open.values() {
            shut();
        }
        if let Some(wake) = wake {
            wake();
        }
    }

    /// Records that connection `id` ended.
    fn ended(&self, id: u64) {
        {
            let mut state = self.lock();
            state.active = state.active.saturating_sub(1);
            let _shut = state.open.remove(&id);
        }
        self.changed.notify_all();
    }
}

/// A handle that stops a [`Server`] from any thread. `Clone`, `Send`,
/// `Sync`.
///
/// # Examples
///
/// ```
/// let server = rpc_io::Server::new(rpc_io::Framing::Newline);
/// let stopper = server.stopper();
/// assert!(!stopper.is_stopped());
/// stopper.stop(); // before serving: the serve call returns at once
/// assert!(stopper.is_stopped());
/// ```
#[derive(Clone)]
pub struct Stopper(Arc<Shared>);

impl Stopper {
    /// Stops the server: no more connections are accepted, every open one is
    /// shut down, and the serve call returns once their threads have
    /// finished. Idempotent.
    ///
    /// # Examples
    ///
    /// See [`Server`].
    pub fn stop(&self) {
        self.0.stop();
    }

    /// Whether [`stop`](Stopper::stop) was called.
    ///
    /// # Examples
    ///
    /// See [`Stopper`].
    #[must_use]
    pub fn is_stopped(&self) -> bool {
        self.0.lock().stopped
    }
}

impl fmt::Debug for Stopper {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stopper")
            .field("stopped", &self.is_stopped())
            .finish()
    }
}

/// A connected stream the server can split, time out, and shut down.
trait Stream: Read + Write + Send + Sync + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn shut(&self);

    /// Prepares the next read to end by `deadline`: `false` if the deadline
    /// has passed (or the server stopped). Where shutting the socket down
    /// wakes a blocked read (Unix), this only sets the read timeout to what
    /// is left, and a read that times out closes the connection.
    fn wait_readable(
        &self,
        deadline: Option<Instant>,
        _stopped: &dyn Fn() -> bool,
    ) -> io::Result<bool> {
        let left = match deadline {
            None => None,
            Some(at) => match at.checked_duration_since(Instant::now()) {
                Some(left) if !left.is_zero() => Some(left),
                _ => return Ok(false),
            },
        };
        self.set_read_timeout(left)?;
        Ok(true)
    }
}

impl Stream for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        TcpStream::try_clone(self)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_write_timeout(self, timeout.filter(|t| !t.is_zero()))
    }

    fn shut(&self) {
        // Already closed by the peer is fine: either way it is over.
        let _closed = self.shutdown(Shutdown::Both);
    }

    /// Windows: shutting the socket down does not wake a blocked receive,
    /// and a receive that times out leaves the socket undefined. So the wait
    /// is a timed `peek` of one byte, repeated every [`TICK`] to look for a
    /// stop: a peek consumes nothing, so its timeout cannot lose data, and
    /// the read that follows finds data (or the end of the stream) waiting.
    #[cfg(windows)]
    fn wait_readable(
        &self,
        deadline: Option<Instant>,
        stopped: &dyn Fn() -> bool,
    ) -> io::Result<bool> {
        let mut byte = [0u8; 1];
        loop {
            if stopped() {
                return Ok(false);
            }
            let tick = match deadline {
                None => TICK,
                Some(at) => match at.checked_duration_since(Instant::now()) {
                    Some(left) if !left.is_zero() => left.min(TICK),
                    _ => return Ok(false),
                },
            };
            self.set_read_timeout(Some(tick))?;
            match self.peek(&mut byte) {
                Ok(_) => return Ok(true),
                Err(error) if is_timeout(&error) || error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(unix)]
impl Stream for std::os::unix::net::UnixStream {
    fn try_clone(&self) -> io::Result<Self> {
        std::os::unix::net::UnixStream::try_clone(self)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        std::os::unix::net::UnixStream::set_write_timeout(self, timeout.filter(|t| !t.is_zero()))
    }

    fn shut(&self) {
        let _closed = self.shutdown(Shutdown::Both);
    }
}

/// Whether an I/O error is a read or write timing out.
fn is_timeout(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
    )
}

/// The writing side of a connection: a write that fails (or times out)
/// shuts the connection down, so the reader stops at once instead of at its
/// next deadline.
struct Writer<T> {
    stream: T,
    closer: Arc<T>,
}

impl<T: Stream> Write for Writer<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream.write(buf).inspect_err(|error| {
            if error.kind() != io::ErrorKind::Interrupted {
                self.closer.shut();
            }
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush().inspect_err(|_| self.closer.shut())
    }
}

/// Why a connection ended, for the log (read only through `Debug`, by the
/// `tracing` feature).
#[allow(dead_code)]
#[derive(Debug)]
enum Ending {
    /// The input ended, or a handler stopped the endpoint.
    Done(End),
    /// A deadline passed: `message_timeout` or `idle_timeout`.
    Late,
    /// The server was stopped.
    Stopped,
    /// The connection failed.
    Failed(Error),
}

/// Serves one connection until it ends: reads with its deadlines, and
/// stops at the first one that passes.
fn serve_connection<T: Stream, S>(
    stream: &mut T,
    endpoint: &mut Endpoint<S, Queued>,
    framing: Framing,
    deadlines: Deadlines,
    shared: &Shared,
) -> Ending {
    let stopped = || shared.lock().stopped;
    let handled = |endpoint: &Endpoint<S, Queued>| {
        let stats = endpoint.stats();
        stats.bodies_in.wrapping_add(stats.invalid_in)
    };
    // A new connection is held to the message deadline until its first
    // message: connecting and sending nothing does not hold a slot for long.
    let mut busy_since = Some(Instant::now());
    let mut last_read = Instant::now();
    let mut done = 0;
    loop {
        match endpoint.next_read(framing) {
            Ok(Next::Read(_)) => {}
            Ok(Next::Stopped) => return Ending::Done(End::Stopped),
            Err(error) => return Ending::Failed(error),
        }
        let finished = handled(endpoint);
        if endpoint.buffered_input() > 0 {
            // Part of a message: due `message_timeout` after its first bytes.
            busy_since = busy_since.or(Some(last_read));
        } else if finished != done {
            done = finished;
            busy_since = None;
        }
        let deadline = match busy_since {
            Some(since) => deadlines.message.and_then(|t| since.checked_add(t)),
            None => deadlines.idle.and_then(|t| last_read.checked_add(t)),
        };
        match stream.wait_readable(deadline, &stopped) {
            Ok(true) => {}
            Ok(false) if stopped() => return Ending::Stopped,
            Ok(false) => return Ending::Late,
            Err(error) => return Ending::Failed(Error::Io(error)),
        }
        let Ok(Next::Read(buf)) = endpoint.next_read(framing) else {
            continue;
        };
        match stream.read(buf) {
            Ok(0) => {
                return match endpoint.finish() {
                    Ok(end) => Ending::Done(end),
                    Err(error) => Ending::Failed(error),
                };
            }
            Ok(n) => {
                endpoint.commit(n);
                last_read = Instant::now();
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            // A read that timed out leaves the socket undefined on some
            // platforms: the connection is closed, never read again.
            Err(error) if is_timeout(&error) => {
                return if stopped() {
                    Ending::Stopped
                } else {
                    Ending::Late
                };
            }
            Err(error) => return Ending::Failed(Error::Io(error)),
        }
    }
}

impl Server {
    /// A server for connections that speak `framing`, with the defaults in
    /// the table above.
    ///
    /// # Examples
    ///
    /// ```
    /// let server = rpc_io::Server::new(rpc_io::Framing::ContentLength);
    /// assert_eq!(server.max_connections(), 256);
    /// ```
    #[must_use]
    pub fn new(framing: Framing) -> Self {
        Self {
            framing,
            config: Config::new(),
            queue: QueueLimits::new(),
            max_connections: 256,
            deadlines: Deadlines {
                message: Some(Duration::from_secs(30)),
                idle: Some(Duration::from_secs(300)),
            },
            shared: Arc::default(),
        }
    }

    /// Sets how long a message may take to arrive once its first bytes
    /// have, and a new connection to send its first message (`None`: no
    /// limit). A connection past it is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let server = rpc_io::Server::new(rpc_io::Framing::Newline).with_message_timeout(Some(Duration::from_secs(5)));
    /// assert_eq!(server.message_timeout(), Some(Duration::from_secs(5)));
    /// ```
    #[must_use]
    pub fn with_message_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.deadlines.message = timeout;
        self
    }

    /// Sets how long a connection may stay silent between messages (`None`:
    /// no limit). A connection past it is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// let server = rpc_io::Server::new(rpc_io::Framing::Newline).with_idle_timeout(None);
    /// assert_eq!(server.idle_timeout(), None);
    /// ```
    #[must_use]
    pub fn with_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.deadlines.idle = timeout;
        self
    }

    /// The message timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// assert_eq!(rpc_io::Server::new(rpc_io::Framing::Newline).message_timeout(), Some(Duration::from_secs(30)));
    /// ```
    #[must_use]
    pub fn message_timeout(&self) -> Option<Duration> {
        self.deadlines.message
    }

    /// The idle timeout.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// assert_eq!(rpc_io::Server::new(rpc_io::Framing::Newline).idle_timeout(), Some(Duration::from_secs(300)));
    /// ```
    #[must_use]
    pub fn idle_timeout(&self) -> Option<Duration> {
        self.deadlines.idle
    }

    /// Sets every endpoint's [`Config`]. For peers across a network, start
    /// from [`Limits::network`](crate::Limits::network).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, Framing, Limits, Server};
    ///
    /// let server = Server::new(Framing::Newline).with_config(Config::new().with_limits(Limits::network()));
    /// # let _ = server;
    /// ```
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Sets every connection's outgoing [`QueueLimits`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, QueueLimits, Server};
    ///
    /// let server = Server::new(Framing::Newline).with_queue(QueueLimits::new().with_max_bytes(1 << 20));
    /// # let _ = server;
    /// ```
    #[must_use]
    pub fn with_queue(mut self, queue: QueueLimits) -> Self {
        self.queue = queue;
        self
    }

    /// Sets how many connections are served at once (at least one).
    ///
    /// # Examples
    ///
    /// ```
    /// let server = rpc_io::Server::new(rpc_io::Framing::Newline).with_max_connections(0);
    /// assert_eq!(server.max_connections(), 1);
    /// ```
    #[must_use]
    pub fn with_max_connections(mut self, count: usize) -> Self {
        self.max_connections = count.max(1);
        self
    }

    /// How many connections are served at once.
    ///
    /// # Examples
    ///
    /// See [`with_max_connections`](Server::with_max_connections).
    #[must_use]
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// Connections being served now.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Server::new(rpc_io::Framing::Newline).connections(), 0);
    /// ```
    #[must_use]
    pub fn connections(&self) -> usize {
        self.shared.lock().active
    }

    /// A handle that stops the server from any thread.
    ///
    /// # Examples
    ///
    /// See [`Server`] and [`Stopper`].
    #[must_use]
    pub fn stopper(&self) -> Stopper {
        Stopper(Arc::clone(&self.shared))
    }

    /// Accepts TCP connections from `listener` and serves each with the
    /// router and state `connect` makes for it, until stopped.
    ///
    /// `connect` runs on the accepting thread, once per connection, before
    /// any of its messages is read.
    ///
    /// # Errors
    ///
    /// The I/O error that ended the accept loop: reading the listener's
    /// address, or an `accept` failure that is not about one connection or a
    /// passing shortage. An aborted or reset connection is skipped; running
    /// out of file descriptors, buffers, or memory (`EMFILE`, `ENFILE`,
    /// `ENOBUFS`, `ENOMEM`, and their Windows counterparts) pauses the loop,
    /// from 10 ms doubling to 1 s, and it carries on: open connections keep
    /// being served and free resources as they end. Every open connection is
    /// shut down before an error is returned. A connection's own failures end
    /// only that connection.
    ///
    /// # Examples
    ///
    /// Per-connection state, numbered by the server:
    ///
    /// ```no_run
    /// use std::net::TcpListener;
    /// use rpc_io::{Framing, Router, Server};
    ///
    /// let listener = TcpListener::bind("127.0.0.1:7000")?;
    /// Server::new(Framing::ContentLength).serve_tcp(listener, |connection| {
    ///     let router = Router::new().request("whoami", |me: &mut u64, _, _| Ok(*me));
    ///     (router, connection.id)
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn serve_tcp<S, F>(&self, listener: TcpListener, connect: F) -> io::Result<()>
    where
        S: Send + 'static,
        F: FnMut(&Connection) -> (Router<S>, S),
    {
        let local = listener.local_addr()?;
        let wake = move || {
            let _woken = TcpStream::connect_timeout(&reachable(local), WAKE_TIMEOUT);
        };
        self.serve(
            || {
                listener
                    .accept()
                    .map(|(stream, addr)| (stream, addr.to_string()))
            },
            Box::new(wake),
            connect,
        )
    }

    /// Accepts connections on the Unix domain socket `listener` and serves
    /// each, as [`serve_tcp`](Server::serve_tcp). Unix only.
    ///
    /// # Errors
    ///
    /// As [`serve_tcp`](Server::serve_tcp); also `InvalidInput` for a
    /// listener with no path (the stopper wakes the loop by connecting to
    /// it).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::os::unix::net::UnixListener;
    /// use rpc_io::{Framing, Router, Server};
    ///
    /// let listener = UnixListener::bind("/tmp/rpc-io.sock")?;
    /// Server::new(Framing::Newline).serve_unix(listener, |_connection| {
    ///     (Router::new().request("ping", |_: &mut (), _, _| Ok("pong")), ())
    /// })?;
    /// # Ok::<(), std::io::Error>(())
    /// ```
    #[cfg(unix)]
    #[cfg_attr(docsrs, doc(cfg(unix)))]
    pub fn serve_unix<S, F>(
        &self,
        listener: std::os::unix::net::UnixListener,
        connect: F,
    ) -> io::Result<()>
    where
        S: Send + 'static,
        F: FnMut(&Connection) -> (Router<S>, S),
    {
        use std::os::unix::net::UnixStream;
        let path = listener
            .local_addr()?
            .as_pathname()
            .map(std::path::Path::to_path_buf)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the socket has no path to wake it with",
                )
            })?;
        let wake = move || {
            let _woken = UnixStream::connect(&path);
        };
        self.serve(
            || {
                listener.accept().map(|(stream, addr)| {
                    let addr = addr
                        .as_pathname()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default();
                    (stream, addr)
                })
            },
            Box::new(wake),
            connect,
        )
    }

    /// The accept loop, over any listener.
    fn serve<T, S, A, F>(
        &self,
        mut accept: A,
        wake: Box<dyn Fn() + Send>,
        mut connect: F,
    ) -> io::Result<()>
    where
        T: Stream,
        S: Send + 'static,
        A: FnMut() -> io::Result<(T, String)>,
        F: FnMut(&Connection) -> (Router<S>, S),
    {
        if !self.arm(wake) {
            return Ok(());
        }
        let mut threads: Vec<JoinHandle<()>> = Vec::new();
        let mut next = 0u64;
        let mut backoff = BACKOFF.0;
        let result = loop {
            if !self.wait_for_room() {
                break Ok(());
            }
            let (stream, addr) = match accept() {
                Ok(accepted) => {
                    backoff = BACKOFF.0;
                    accepted
                }
                Err(error) if transient(&error) => continue,
                Err(_error) if exhausted(&_error) => {
                    event!(warn, error = %_error, pause = ?backoff, "accept is out of resources; pausing");
                    if !self.pause(backoff) {
                        break Ok(());
                    }
                    backoff = (backoff * 2).min(BACKOFF.1);
                    continue;
                }
                Err(error) => break Err(error),
            };
            if self.shared.lock().stopped {
                // The stopper's own wake-up connection, or one that raced it.
                break Ok(());
            }
            let connection = Connection { id: next, addr };
            next = next.wrapping_add(1);
            let (router, state) = connect(&connection);
            match self.spawn(stream, &connection, router, state) {
                Ok(Some(thread)) => threads.push(thread),
                Ok(None) => break Ok(()),
                Err(_error) => {
                    event!(warn, error = %_error, "a connection could not be served");
                }
            }
            threads.retain(|thread| !thread.is_finished());
        };
        self.shared.stop();
        for thread in threads {
            // Each thread contains its endpoint's panics: it cannot fail.
            let _joined = thread.join();
        }
        result
    }

    /// Installs the wake-up for the stopper; `false` if already stopped.
    fn arm(&self, wake: Box<dyn Fn() + Send>) -> bool {
        let mut state = self.shared.lock();
        if state.stopped {
            return false;
        }
        state.wake = Some(wake);
        true
    }

    /// Sleeps for `pause`, or until stopped; `false` once stopped.
    fn pause(&self, pause: Duration) -> bool {
        let deadline = Instant::now() + pause;
        let mut state = self.shared.lock();
        while !state.stopped {
            let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                break;
            };
            state = self
                .shared
                .changed
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        !state.stopped
    }

    /// Waits until fewer than `max_connections` are open; `false` once
    /// stopped.
    fn wait_for_room(&self) -> bool {
        let mut state = self.shared.lock();
        while !state.stopped && state.active >= self.max_connections {
            state = self
                .shared
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        !state.stopped
    }

    /// Serves one connection on a thread of its own; `None` if the server
    /// was stopped meanwhile (the connection is closed unserved).
    fn spawn<T: Stream, S: Send + 'static>(
        &self,
        stream: T,
        connection: &Connection,
        router: Router<S>,
        state: S,
    ) -> io::Result<Option<JoinHandle<()>>> {
        let closer = Arc::new(stream.try_clone()?);
        let writer = Writer {
            stream: stream.try_clone()?,
            closer: Arc::clone(&closer),
        };
        // A peer that stops reading fails the write itself, which shuts the
        // connection down (see `Writer`).
        stream.set_write_timeout(self.queue.write_timeout)?;
        let sink = Queued::with_limits(writer, self.framing, self.queue)?;
        let (framing, config, shared, id, deadlines) = (
            self.framing,
            self.config.clone(),
            Arc::clone(&self.shared),
            connection.id,
            self.deadlines,
        );
        {
            // Registered under the lock the stopper takes: a connection is
            // either shut down by a stop or not served at all.
            let mut open = self.shared.lock();
            if open.stopped {
                closer.shut();
                return Ok(None);
            }
            open.active += 1;
            let registered = Arc::clone(&closer);
            let _fresh = open.open.insert(id, Box::new(move || registered.shut()));
        }
        let ending = Arc::clone(&closer);
        let started = thread::Builder::new()
            .name(format!("rpc-io-connection-{id}"))
            .spawn(move || {
                let mut endpoint = Endpoint::new(sink, router, state).with_config(config);
                let mut stream = stream;
                let ended =
                    serve_connection(&mut stream, &mut endpoint, framing, deadlines, &shared);
                event!(debug, connection = id, end = ?ended, "a connection ended");
                // Closing the endpoint drains its queue; then the stream ends.
                drop((endpoint, ended));
                ending.shut();
                shared.ended(id);
            });
        match started {
            Ok(thread) => Ok(Some(thread)),
            Err(error) => {
                closer.shut();
                self.shared.ended(id);
                Err(error)
            }
        }
    }
}

impl fmt::Debug for Server {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Server")
            .field("framing", &self.framing)
            .field("config", &self.config)
            .field("queue", &self.queue)
            .field("max_connections", &self.max_connections)
            .field("message_timeout", &self.deadlines.message)
            .field("idle_timeout", &self.deadlines.idle)
            .field("connections", &self.connections())
            .finish_non_exhaustive()
    }
}

/// `addr`, with an unspecified IP (`0.0.0.0`, `::`) replaced by loopback,
/// which a listener bound to it also accepts on.
fn reachable(addr: SocketAddr) -> SocketAddr {
    let mut addr = addr;
    if addr.ip().is_unspecified() {
        addr.set_ip(match addr {
            SocketAddr::V4(_) => std::net::Ipv4Addr::LOCALHOST.into(),
            SocketAddr::V6(_) => std::net::Ipv6Addr::LOCALHOST.into(),
        });
    }
    addr
}

/// Whether an `accept` error is the system running short of descriptors,
/// buffers, or memory: worth waiting out, not ending the server for.
fn exhausted(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::OutOfMemory {
        return true;
    }
    let Some(code) = error.raw_os_error() else {
        return false;
    };
    // ENOMEM, ENFILE, EMFILE, and ENOBUFS (105 on Linux, 55 on the BSDs and
    // macOS); WSAEMFILE and WSAENOBUFS on Windows.
    if cfg!(windows) {
        matches!(code, 10024 | 10055)
    } else if cfg!(target_os = "linux") || cfg!(target_os = "android") {
        matches!(code, 12 | 23 | 24 | 105)
    } else {
        matches!(code, 12 | 23 | 24 | 55)
    }
}

/// Whether an `accept` error concerns one connection only.
fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_exhaustion_is_waited_out_and_other_errors_are_not() {
        let out_of_files = if cfg!(windows) { 10024 } else { 24 };
        assert!(exhausted(&io::Error::from_raw_os_error(out_of_files)));
        assert!(exhausted(&io::Error::from(io::ErrorKind::OutOfMemory)));
        assert!(!exhausted(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(!exhausted(&io::Error::other("no code")));
        assert!(transient(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
        assert!(!transient(&io::Error::from(io::ErrorKind::OutOfMemory)));
    }

    #[test]
    fn test_pause_ends_early_on_a_stop() {
        let server = Server::new(Framing::Newline);
        let started = Instant::now();
        assert!(server.pause(Duration::from_millis(20)));
        assert!(started.elapsed() >= Duration::from_millis(15));
        let stopper = server.stopper();
        let waker = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            stopper.stop();
        });
        let started = Instant::now();
        assert!(!server.pause(Duration::from_secs(30)));
        assert!(started.elapsed() < Duration::from_secs(10));
        assert!(waker.join().is_ok());
    }

    #[test]
    fn test_reachable_replaces_unspecified_addresses() {
        let v4: SocketAddr = ([0, 0, 0, 0], 9).into();
        assert_eq!(reachable(v4).ip(), std::net::Ipv4Addr::LOCALHOST);
        let v6: SocketAddr = (std::net::Ipv6Addr::UNSPECIFIED, 9).into();
        assert_eq!(reachable(v6).ip(), std::net::Ipv6Addr::LOCALHOST);
        let set: SocketAddr = ([10, 0, 0, 1], 9).into();
        assert_eq!(reachable(set), set);
    }
}
