//! [`Server`]: an accept loop that serves one [`Endpoint`] per connection,
//! over TCP or (on Unix) a Unix domain socket. The `net` feature.

use std::{
    collections::BTreeMap,
    fmt,
    io::{self, Read, Write},
    net::{Shutdown, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    thread::{self, JoinHandle},
    time::Duration,
};

use crate::{
    config::Config,
    endpoint::Endpoint,
    framing::Framing,
    queue::{QueueLimits, Queued},
    router::Router,
    trace::event,
};

/// How long the stopper waits to wake a blocked `accept` by connecting.
const WAKE_TIMEOUT: Duration = Duration::from_secs(1);

/// How often a connection's reader looks for a stop while its peer is
/// silent.
const STOP_POLL: Duration = Duration::from_millis(100);

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
///
/// Concurrency is bounded: at `max_connections`, the loop stops accepting
/// (the operating system queues further connections in its backlog) until a
/// connection ends. Each connection costs a reader thread, a writer thread,
/// and, once a request with a deadline is sent on it, a timer thread.
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
    shared: Arc<Shared>,
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
    fn shut(&self);
}

impl Stream for TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        TcpStream::try_clone(self)
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn shut(&self) {
        // Already closed by the peer is fine: either way it is over.
        let _closed = self.shutdown(Shutdown::Both);
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

    fn shut(&self) {
        let _closed = self.shutdown(Shutdown::Both);
    }
}

/// The reading side of a connection. Shutting a socket down does not wake a
/// read blocked on it on every platform (not on Windows), so reads time out
/// every [`STOP_POLL`] to look for a stop; while the server runs, a timeout
/// is retried and never reaches the endpoint.
struct Watched<T> {
    stream: T,
    shared: Arc<Shared>,
}

impl<T: Stream> Read for Watched<T> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            match self.stream.read(buf) {
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    if self.shared.lock().stopped {
                        return Ok(0);
                    }
                }
                read => return read,
            }
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
            shared: Arc::default(),
        }
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
    /// address, or an `accept` failure other than an aborted or reset
    /// connection (which are skipped). Every open connection is shut down
    /// first. A connection's own failures end only that connection.
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
        let result = loop {
            if !self.wait_for_room() {
                break Ok(());
            }
            let (stream, addr) = match accept() {
                Ok(accepted) => accepted,
                Err(error) if transient(&error) => continue,
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
        let writer = stream.try_clone()?;
        let closer = Arc::new(stream.try_clone()?);
        stream.set_read_timeout(Some(STOP_POLL))?;
        let reader = Watched {
            stream,
            shared: Arc::clone(&self.shared),
        };
        let sink = Queued::with_limits(writer, self.framing, self.queue)?;
        let (framing, config, shared, id) = (
            self.framing,
            self.config.clone(),
            Arc::clone(&self.shared),
            connection.id,
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
                let ended = endpoint.run(reader, framing);
                event!(debug, connection = id, end = ?ended, "a connection ended");
                // Dropping the endpoint drains its queue; then the stream ends.
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

/// Whether an `accept` error concerns one connection only.
fn transient(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::Interrupted
    )
}
