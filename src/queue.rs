//! Sinks whose senders do not wait on the peer: [`Queued`] (a writer thread)
//! and [`outbox`] (an async task). Messages are framed into a bounded queue
//! under [`QueueLimits`]; whoever drains it does the slow part, the write to
//! the peer, without the endpoint's lock. Closing waits for the drain only
//! after the lock is released, and for at most `close_timeout`.

use std::{
    fmt,
    io::{self, Write},
    mem,
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::{
    framing::Framing,
    sink::{Drain, Sink},
};

/// A drained buffer grown past this by one large batch is released.
const KEEP: usize = 64 * 1024;

/// The writer thread hands the writer at most this much at a time, so that
/// progress is seen (and the no-progress timeout restarted) as it is made.
const WRITE_CHUNK: usize = 64 * 1024;

/// How long a closing [`Queued`] sleeps between checks on its writer.
const DRAIN_POLL: Duration = Duration::from_millis(50);

/// Bounds on an outgoing queue: what it may hold, how long the peer may stop
/// taking bytes, and how long closing waits for what is left.
///
/// | Bound | Default | Meaning |
/// |---|---:|---|
/// | `max_bytes` | 64 MiB | framed bytes queued or being written |
/// | `max_messages` | 65,536 | messages queued or being written |
/// | `write_timeout` | 60 s | how long the writer may go without any byte being written |
/// | `close_timeout` | 10 s | how long closing waits for the queue to drain |
///
/// A message that would take the queue past `max_bytes` or `max_messages`
/// fails the queue (one message is always taken when the queue is empty, so
/// a single large message still goes out). `write_timeout` is a no-progress
/// timeout: the writer writes at most 64 KiB at a time and every byte the
/// peer takes restarts it, so a slow but steady reader is never cut off. It
/// is checked whenever a message is sent, the endpoint flushes, or the queue
/// closes (a writer blocked in a write cannot check it itself; give the
/// writer a write timeout of its own, as [`Server`](crate::Server) does with
/// its sockets, to have the write itself fail). A failed queue refuses
/// everything with the same I/O error, which closes the endpoint. A peer that
/// stops reading therefore costs at most `max_bytes` of memory, and the
/// connection is closed by the first send after `write_timeout`.
///
/// `#[non_exhaustive]`: start from [`QueueLimits::new`] and chain `with_*`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use rpc_io::QueueLimits;
///
/// let tight = QueueLimits::new()
///     .with_max_bytes(1 << 20)
///     .with_write_timeout(Some(Duration::from_secs(5)));
/// assert_eq!(tight.max_messages, 65_536);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct QueueLimits {
    /// Most framed bytes queued or being written.
    pub max_bytes: usize,
    /// Most messages queued or being written.
    pub max_messages: usize,
    /// The longest the writer may go without writing a byte while it has
    /// bytes to write; `None` waits forever.
    pub write_timeout: Option<Duration>,
    /// The longest closing waits for what is queued to be written, after
    /// which the writer is left to finish (or fail) on its own.
    pub close_timeout: Duration,
}

impl QueueLimits {
    /// The defaults in the table above.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::QueueLimits::new().max_bytes, 64 * 1024 * 1024);
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024,
            max_messages: 65_536,
            write_timeout: Some(Duration::from_secs(60)),
            close_timeout: Duration::from_secs(10),
        }
    }

    /// Sets `max_bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::QueueLimits::new().with_max_bytes(10).max_bytes, 10);
    /// ```
    #[must_use]
    pub const fn with_max_bytes(mut self, bytes: usize) -> Self {
        self.max_bytes = bytes;
        self
    }

    /// Sets `max_messages`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::QueueLimits::new().with_max_messages(3).max_messages, 3);
    /// ```
    #[must_use]
    pub const fn with_max_messages(mut self, count: usize) -> Self {
        self.max_messages = count;
        self
    }

    /// Sets `write_timeout`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::QueueLimits::new().with_write_timeout(None).write_timeout, None);
    /// ```
    #[must_use]
    pub const fn with_write_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.write_timeout = timeout;
        self
    }

    /// Sets `close_timeout`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let limits = rpc_io::QueueLimits::new().with_close_timeout(Duration::from_secs(1));
    /// assert_eq!(limits.close_timeout, Duration::from_secs(1));
    /// ```
    #[must_use]
    pub const fn with_close_timeout(mut self, timeout: Duration) -> Self {
        self.close_timeout = timeout;
        self
    }
}

impl Default for QueueLimits {
    fn default() -> Self {
        Self::new()
    }
}

/// The queue both ends share.
struct Core {
    framing: Framing,
    limits: QueueLimits,
    state: Mutex<State>,
    /// Wakes the writer thread (more to write, or closing) and whoever waits
    /// for the queue to drain.
    signal: Condvar,
}

struct State {
    /// Framed messages not yet taken by the writer.
    queued: Vec<u8>,
    queued_messages: usize,
    /// Bytes and messages taken by the writer and not yet written.
    writing: usize,
    writing_messages: usize,
    /// While the writer has bytes to write: when it last wrote any (or took
    /// the batch).
    progressed: Option<Instant>,
    /// Why the queue failed: the writer's error, a stall, or an overflow.
    failed: Option<(io::ErrorKind, String)>,
    /// The sending side is done: drain, then stop.
    closed: bool,
    /// The writer has stopped.
    finished: bool,
    /// The async writer's task, while it waits for messages.
    waker: Option<Waker>,
    /// The writer thread is waiting for messages: only then does a send
    /// need to wake it (a busy writer takes everything queued when its
    /// write returns).
    idle: bool,
}

impl Core {
    fn new(framing: Framing, limits: QueueLimits) -> Arc<Self> {
        Arc::new(Self {
            framing,
            limits,
            state: Mutex::new(State {
                queued: Vec::new(),
                queued_messages: 0,
                writing: 0,
                writing_messages: 0,
                progressed: None,
                failed: None,
                closed: false,
                finished: false,
                waker: None,
                idle: false,
            }),
            signal: Condvar::new(),
        })
    }

    /// The state. Nothing that runs under this lock can panic, so the poison
    /// is ignored.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Frames and queues `message`, or fails the queue.
    fn push(&self, message: &str) -> io::Result<()> {
        let mut state = self.lock();
        state.check(&self.limits)?;
        let bytes = state.queued.len() + state.writing;
        let messages = state.queued_messages + state.writing_messages;
        let before = state.queued.len();
        self.framing.encode(message.as_bytes(), &mut state.queued);
        let added = state.queued.len() - before;
        let over = bytes.saturating_add(added) > self.limits.max_bytes
            || messages.saturating_add(1) > self.limits.max_messages;
        if messages > 0 && over {
            state.queued.truncate(before);
            return Err(state.fail(
                io::ErrorKind::Other,
                "the output queue is full: the peer is not reading".into(),
            ));
        }
        state.queued_messages += 1;
        let waker = state.waker.take();
        let idle = std::mem::take(&mut state.idle);
        drop(state);
        if idle {
            self.signal.notify_all();
        }
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// The queue's failure, if any (including a write stalled too long).
    fn check(&self) -> io::Result<()> {
        self.lock().check(&self.limits)
    }

    /// Ends the sending side: what is queued is still written.
    fn close(&self) {
        let waker = {
            let mut state = self.lock();
            state.closed = true;
            state.waker.take()
        };
        self.signal.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Waits until the writer has stopped, for at most `close_timeout`, and
    /// no longer once it has made no progress for `write_timeout`; `false`
    /// if it gave up.
    fn wait_drained(&self) -> bool {
        let deadline = Instant::now().checked_add(self.limits.close_timeout);
        let mut state = self.lock();
        while !state.finished {
            let now = Instant::now();
            let late = deadline.is_none_or(|at| now >= at);
            if late || state.stalled(&self.limits).is_some() {
                return false;
            }
            let left = deadline.map_or(DRAIN_POLL, |at| (at - now).min(DRAIN_POLL));
            state = self
                .signal
                .wait_timeout(state, left)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
        true
    }

    /// Records that the writer wrote some bytes.
    fn progress(&self) {
        let mut state = self.lock();
        if state.progressed.is_some() {
            state.progressed = Some(Instant::now());
        }
    }

    /// Bytes and messages queued or being written.
    fn pending(&self) -> (usize, usize) {
        let state = self.lock();
        (
            state.queued.len() + state.writing,
            state.queued_messages + state.writing_messages,
        )
    }
}

impl State {
    fn check(&mut self, limits: &QueueLimits) -> io::Result<()> {
        if let Some((kind, message)) = &self.failed {
            return Err(io::Error::new(*kind, message.clone()));
        }
        if let Some(limit) = self.stalled(limits) {
            return Err(self.fail(
                io::ErrorKind::TimedOut,
                format!(
                    "a write to the peer made no progress for {limit:?}: the peer is not reading"
                ),
            ));
        }
        Ok(())
    }

    /// The write timeout, if the writer has written nothing for longer.
    fn stalled(&self, limits: &QueueLimits) -> Option<Duration> {
        let since = self.progressed?;
        let limit = limits.write_timeout?;
        (since.elapsed() > limit).then_some(limit)
    }

    /// Fails the queue for good (the first reason wins) and drops what is
    /// queued: nothing more can reach the peer in order.
    fn fail(&mut self, kind: io::ErrorKind, message: String) -> io::Error {
        let (kind, message) = self.failed.get_or_insert((kind, message)).clone();
        self.queued = Vec::new();
        self.queued_messages = 0;
        io::Error::new(kind, message)
    }

    /// Takes everything queued into `batch` (empty on entry) for writing.
    fn take(&mut self, batch: &mut Vec<u8>) {
        mem::swap(&mut self.queued, batch);
        self.writing = batch.len();
        self.writing_messages = mem::take(&mut self.queued_messages);
        self.progressed = Some(Instant::now());
    }

    /// Records that the batch taken last was written (or abandoned).
    fn written(&mut self) {
        self.writing = 0;
        self.writing_messages = 0;
        self.progressed = None;
    }
}

/// Empties a drained batch buffer, releasing it if one large batch grew it.
fn recycle(batch: &mut Vec<u8>) {
    batch.clear();
    if batch.capacity() > 2 * KEEP {
        batch.shrink_to(KEEP);
    }
}

/// A [`Sink`] that frames messages into a bounded queue and writes them to a
/// byte stream from its own thread.
///
/// A send never waits on the peer: the endpoint's lock covers framing a
/// message into the queue, and the writer thread does the writes (in chunks
/// of at most 64 KiB) and one `flush` per batch of queued messages without
/// it. A peer that stops reading therefore stalls no sender: the queue grows
/// to its [`QueueLimits`], then the connection closes. This is the sink
/// [`serve`] uses, and the one to use for any peer that might be slow.
///
/// Closing the endpoint closes the queue without waiting, under the
/// endpoint's lock; then, with the lock released, the endpoint waits for what
/// is queued to be written: until the writer is done, has made no progress
/// for `write_timeout`, or `close_timeout` has passed. Handles on other
/// threads see the endpoint closed at once and are never held up by this
/// wait. When the writer finishes, its thread ends and the writer is dropped,
/// which ends the stream for the peer; a writer still stuck in a write when
/// the wait gives up ends when that write returns.
///
/// [`serve`]: crate::serve
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Framing, Queued, Router};
///
/// let output = std::io::stdout(); // a socket, a pipe: anything `Write + Send`
/// let sink = Queued::new(output, Framing::ContentLength)?;
/// let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
/// assert!(!endpoint.is_closed());
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct Queued {
    core: Arc<Core>,
    writer: Option<JoinHandle<()>>,
}

impl Queued {
    /// Frames messages with `framing` onto `writer` from a new writer thread,
    /// under the default [`QueueLimits`].
    ///
    /// # Errors
    ///
    /// The I/O error from starting the thread.
    ///
    /// # Examples
    ///
    /// ```
    /// let sink = rpc_io::Queued::new(std::io::sink(), rpc_io::Framing::Newline)?;
    /// assert_eq!(sink.framing(), rpc_io::Framing::Newline);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn new<W: Write + Send + 'static>(writer: W, framing: Framing) -> io::Result<Self> {
        Self::with_limits(writer, framing, QueueLimits::new())
    }

    /// As [`new`](Queued::new), under `limits`.
    ///
    /// # Errors
    ///
    /// The I/O error from starting the thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, QueueLimits, Queued};
    ///
    /// let sink = Queued::with_limits(std::io::sink(), Framing::Newline, QueueLimits::new().with_max_messages(8))?;
    /// assert_eq!(sink.limits().max_messages, 8);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn with_limits<W: Write + Send + 'static>(
        writer: W,
        framing: Framing,
        limits: QueueLimits,
    ) -> io::Result<Self> {
        let core = Core::new(framing, limits);
        let shared = Arc::clone(&core);
        let writer = thread::Builder::new()
            .name("rpc-io-writer".into())
            .spawn(move || write_loop(&shared, writer))?;
        Ok(Self {
            core,
            writer: Some(writer),
        })
    }

    /// The framing.
    ///
    /// # Examples
    ///
    /// ```
    /// let sink = rpc_io::Queued::new(Vec::new(), rpc_io::Framing::ContentLength)?;
    /// assert_eq!(sink.framing(), rpc_io::Framing::ContentLength);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    #[must_use]
    pub fn framing(&self) -> Framing {
        self.core.framing
    }

    /// The limits.
    ///
    /// # Examples
    ///
    /// ```
    /// let sink = rpc_io::Queued::new(Vec::new(), rpc_io::Framing::Newline)?;
    /// assert_eq!(sink.limits(), rpc_io::QueueLimits::new());
    /// # Ok::<(), std::io::Error>(())
    /// ```
    #[must_use]
    pub fn limits(&self) -> QueueLimits {
        self.core.limits
    }

    /// Framed bytes queued or being written: how far the peer is behind.
    ///
    /// # Examples
    ///
    /// ```
    /// let sink = rpc_io::Queued::new(Vec::new(), rpc_io::Framing::Newline)?;
    /// assert_eq!(sink.pending_bytes(), 0);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.core.pending().0
    }

    /// Messages queued or being written.
    ///
    /// # Examples
    ///
    /// ```
    /// let sink = rpc_io::Queued::new(Vec::new(), rpc_io::Framing::Newline)?;
    /// assert_eq!(sink.pending_messages(), 0);
    /// # Ok::<(), std::io::Error>(())
    /// ```
    #[must_use]
    pub fn pending_messages(&self) -> usize {
        self.core.pending().1
    }

    /// The wait for the writer: until it is done or gives up, then joins its
    /// thread if it stopped.
    fn waiter(&mut self) -> Drain {
        let core = Arc::clone(&self.core);
        let writer = self.writer.take();
        Drain::new(move || {
            if core.wait_drained() {
                if let Some(writer) = writer {
                    // The thread contains its own panics: it cannot fail to join.
                    let _joined = writer.join();
                }
            }
            // Otherwise the writer is stuck in a write the wait gave up on;
            // the thread ends, and drops the writer, when that write returns.
        })
    }
}

impl Sink for Queued {
    fn send(&mut self, message: &str) -> io::Result<()> {
        self.core.push(message)
    }

    /// Reports the queue's failure, if any: the writer thread writes and
    /// flushes on its own.
    fn flush(&mut self) -> io::Result<()> {
        self.core.check()
    }

    /// Ends the queue without waiting: what is queued is still written.
    fn close(&mut self) -> io::Result<()> {
        let result = self.core.check();
        self.core.close();
        result
    }

    /// Waits, after the endpoint's lock is released, for what is queued to
    /// be written: see [`Queued`].
    fn drain(&mut self) -> Option<Drain> {
        Some(self.waiter())
    }
}

impl Drop for Queued {
    /// Closes the queue and waits for it as [`Sink::drain`] would, unless
    /// the endpoint did so already.
    fn drop(&mut self) {
        self.core.close();
        if self.writer.is_some() {
            self.waiter().wait();
        }
    }
}

impl fmt::Debug for Queued {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (bytes, messages) = self.core.pending();
        f.debug_struct("Queued")
            .field("framing", &self.core.framing)
            .field("limits", &self.core.limits)
            .field("pending_bytes", &bytes)
            .field("pending_messages", &messages)
            .finish_non_exhaustive()
    }
}

/// The writer thread: takes everything queued, writes and flushes it, and
/// repeats until the queue is closed and empty, or fails.
fn write_loop<W: Write>(core: &Core, mut writer: W) {
    let mut batch = Vec::new();
    loop {
        {
            let mut state = core.lock();
            loop {
                if state.failed.is_some() || (state.queued.is_empty() && state.closed) {
                    state.finished = true;
                    drop(state);
                    core.signal.notify_all();
                    return;
                }
                if !state.queued.is_empty() {
                    break;
                }
                state.idle = true;
                state = core
                    .signal
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
                state.idle = false;
            }
            state.take(&mut batch);
        }
        let written = panic::catch_unwind(AssertUnwindSafe(|| {
            write_chunks(core, &mut writer, &batch)?;
            writer.flush()
        }))
        .unwrap_or_else(|_| Err(io::Error::other("the writer panicked")));
        {
            let mut state = core.lock();
            state.written();
            if let Err(error) = written {
                let _failed =
                    state.fail(error.kind(), format!("writing to the peer failed: {error}"));
            }
        }
        core.signal.notify_all();
        recycle(&mut batch);
    }
}

/// Writes all of `batch`, at most [`WRITE_CHUNK`] bytes per call, recording
/// progress after each write that took bytes.
fn write_chunks<W: Write>(core: &Core, writer: &mut W, batch: &[u8]) -> io::Result<()> {
    let mut rest = batch;
    while !rest.is_empty() {
        let chunk = &rest[..rest.len().min(WRITE_CHUNK)];
        match writer.write(chunk) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "the writer accepted no bytes",
                ));
            }
            Ok(n) => {
                rest = &rest[n..];
                core.progress();
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// A sink whose messages an async task writes: the two halves of a bounded
/// outgoing queue.
///
/// The [`OutboxSink`] goes into the [`Endpoint`](crate::Endpoint); the
/// [`Outbox`] goes to a task that writes what it yields to the peer, on any
/// executor. A send does not wait on the peer, and the queue is bounded by
/// `limits`, as for [`Queued`] (`write_timeout` counts from the last
/// [`consume`](Outbox::consume) that took bytes). When the endpoint closes,
/// the outbox yields what is left, then ends; closing does not wait for the
/// task. When the outbox is dropped, the sink fails, which closes the
/// endpoint.
///
/// # Examples
///
/// Draining by polling (an async writer does the same through
/// [`Outbox::poll_chunk`], or `Outbox::write_to` with the `futures-io`
/// feature):
///
/// ```
/// use std::task::{Context, Poll, Waker};
/// use rpc_io::{Endpoint, Framing, QueueLimits, Router};
///
/// let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
/// let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
/// endpoint.peer().notify("ready", ())?;
///
/// let mut cx = Context::from_waker(Waker::noop());
/// let Poll::Ready(chunk) = outbox.poll_chunk(&mut cx) else { panic!() };
/// assert_eq!(chunk, b"{\"jsonrpc\":\"2.0\",\"method\":\"ready\"}\n");
/// let n = chunk.len();
/// outbox.consume(n); // written
/// assert!(outbox.poll_chunk(&mut cx).is_pending()); // nothing more yet
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[must_use]
pub fn outbox(framing: Framing, limits: QueueLimits) -> (OutboxSink, Outbox) {
    let core = Core::new(framing, limits);
    (
        OutboxSink {
            core: Arc::clone(&core),
        },
        Outbox {
            core,
            batch: Vec::new(),
            at: 0,
        },
    )
}

/// The sending half of an [`outbox`]: a [`Sink`] for an endpoint.
///
/// # Examples
///
/// ```
/// use rpc_io::{Framing, QueueLimits, Sink};
///
/// let (mut sink, outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
/// sink.send("{}")?;
/// assert_eq!(outbox.pending_bytes(), 3); // framed, waiting for the task
/// # Ok::<(), std::io::Error>(())
/// ```
pub struct OutboxSink {
    core: Arc<Core>,
}

impl OutboxSink {
    /// Framed bytes queued or being written.
    ///
    /// # Examples
    ///
    /// ```
    /// let (sink, _outbox) = rpc_io::outbox(rpc_io::Framing::Newline, rpc_io::QueueLimits::new());
    /// assert_eq!(sink.pending_bytes(), 0);
    /// ```
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.core.pending().0
    }
}

impl Sink for OutboxSink {
    fn send(&mut self, message: &str) -> io::Result<()> {
        self.core.push(message)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.core.check()
    }

    /// Ends the queue; the outbox yields what is left, then ends. Does not
    /// wait: the async task does the writing.
    fn close(&mut self) -> io::Result<()> {
        let result = self.core.check();
        self.core.close();
        result
    }
}

impl Drop for OutboxSink {
    fn drop(&mut self) {
        self.core.close();
    }
}

impl fmt::Debug for OutboxSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OutboxSink")
            .field("framing", &self.core.framing)
            .field("pending_bytes", &self.pending_bytes())
            .finish_non_exhaustive()
    }
}

/// The writing half of an [`outbox`]: yields framed bytes for the peer.
///
/// Poll [`poll_chunk`](Outbox::poll_chunk) for bytes, write them, and report
/// how many with [`consume`](Outbox::consume), the way a buffered reader is
/// drained. An empty chunk means the end: the endpoint closed and everything
/// was yielded (or the queue failed).
///
/// # Examples
///
/// ```
/// use std::task::{Context, Poll, Waker};
/// use rpc_io::{Endpoint, Framing, QueueLimits, Router};
///
/// let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
/// let mut endpoint = Endpoint::new(sink, Router::<()>::new(), ());
/// endpoint.peer().notify("bye", ())?;
/// endpoint.close();
/// let mut cx = Context::from_waker(Waker::noop());
/// let mut written = Vec::new();
/// while let Poll::Ready(chunk) = outbox.poll_chunk(&mut cx) {
///     if chunk.is_empty() {
///         break; // closed and drained
///     }
///     written.extend_from_slice(chunk);
///     let n = chunk.len();
///     outbox.consume(n);
/// }
/// assert_eq!(written, b"{\"jsonrpc\":\"2.0\",\"method\":\"bye\"}\n");
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Outbox {
    core: Arc<Core>,
    /// The batch being written, and how much of it is.
    batch: Vec<u8>,
    at: usize,
}

impl Outbox {
    /// The next bytes to write: `Ready` with a non-empty chunk, `Ready` with
    /// an empty one at the end, or `Pending` (the task is woken when a
    /// message is sent).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::task::{Context, Poll, Waker};
    /// use rpc_io::{Framing, QueueLimits, Sink};
    ///
    /// let mut cx = Context::from_waker(Waker::noop());
    ///
    /// let (mut sink, mut outbox) = rpc_io::outbox(Framing::ContentLength, QueueLimits::new());
    /// sink.send("{}")?;
    /// drop(sink); // the endpoint is gone: what is queued, then the end
    /// assert_eq!(outbox.poll_chunk(&mut cx), Poll::Ready(&b"Content-Length: 2\r\n\r\n{}"[..]));
    /// outbox.consume(24);
    /// assert_eq!(outbox.poll_chunk(&mut cx), Poll::Ready(&b""[..]));
    /// # Ok::<(), std::io::Error>(())
    /// ```
    pub fn poll_chunk(&mut self, cx: &mut Context<'_>) -> Poll<&[u8]> {
        if self.at < self.batch.len() {
            return Poll::Ready(&self.batch[self.at..]);
        }
        let mut state = self.core.lock();
        if !self.batch.is_empty() {
            state.written();
            recycle(&mut self.batch);
            self.at = 0;
        }
        if state.failed.is_some() {
            state.finished = true;
            return Poll::Ready(&[]);
        }
        if !state.queued.is_empty() {
            state.take(&mut self.batch);
            drop(state);
            return Poll::Ready(&self.batch);
        }
        if state.closed {
            state.finished = true;
            return Poll::Ready(&[]);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }

    /// Records that `n` bytes of the last chunk were written.
    ///
    /// # Examples
    ///
    /// See [`poll_chunk`](Outbox::poll_chunk).
    pub fn consume(&mut self, n: usize) {
        self.at = self.at.saturating_add(n).min(self.batch.len());
        if n > 0 {
            self.core.progress();
        }
    }

    /// Framed bytes queued or being written.
    ///
    /// # Examples
    ///
    /// ```
    /// let (_sink, outbox) = rpc_io::outbox(rpc_io::Framing::Newline, rpc_io::QueueLimits::new());
    /// assert_eq!(outbox.pending_bytes(), 0);
    /// ```
    #[must_use]
    pub fn pending_bytes(&self) -> usize {
        self.core.pending().0
    }

    /// The part of the last chunk not yet consumed.
    #[cfg(feature = "futures-io")]
    pub(crate) fn current(&self) -> &[u8] {
        &self.batch[self.at..]
    }

    /// Fails the queue because writing to the peer failed with `error`, so
    /// the endpoint closes; returns the error.
    #[cfg(feature = "futures-io")]
    pub(crate) fn abandon(&mut self, error: io::Error) -> io::Error {
        let _failed = self
            .core
            .lock()
            .fail(error.kind(), format!("writing to the peer failed: {error}"));
        error
    }
}

impl Drop for Outbox {
    fn drop(&mut self) {
        // Nothing will reach the peer any more: the endpoint must close.
        let mut state = self.core.lock();
        state.finished = true;
        let _failed = state.fail(
            io::ErrorKind::BrokenPipe,
            "the outbox was dropped: nothing reaches the peer".into(),
        );
        drop(state);
        self.core.signal.notify_all();
    }
}

impl fmt::Debug for Outbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Outbox")
            .field("framing", &self.core.framing)
            .field("pending_bytes", &self.pending_bytes())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::{
        sync::mpsc,
        time::{Duration, Instant},
    };

    use super::*;

    /// A writer that blocks until told to go on, then records what it got.
    struct Gate {
        go: mpsc::Receiver<()>,
        out: mpsc::Sender<Vec<u8>>,
    }

    impl Write for Gate {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.go.recv().is_err() {
                return Err(io::Error::other("released"));
            }
            let _sent = self.out.send(buf.to_vec());
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_queue_overflow_fails_the_queue_but_admits_one_message() {
        let (mut sink, _outbox) = outbox(
            Framing::Newline,
            QueueLimits::new().with_max_bytes(4).with_max_messages(10),
        );
        // Larger than the limit, but the queue is empty: taken.
        assert!(sink.send("123456").is_ok());
        let error = sink.send("1").err().map(|e| e.to_string());
        assert_eq!(
            error.as_deref(),
            Some("the output queue is full: the peer is not reading")
        );
        // Failed for good, with the same error.
        assert!(sink.flush().is_err());
        assert!(sink.send("").is_err());
    }

    #[test]
    fn test_message_limit() {
        let (mut sink, _outbox) = outbox(Framing::Newline, QueueLimits::new().with_max_messages(2));
        assert!(sink.send("1").is_ok() && sink.send("2").is_ok());
        assert!(sink.send("3").is_err());
    }

    #[test]
    fn test_outbox_stall_times_out_the_sender() {
        let mut cx = Context::from_waker(Waker::noop());
        let (mut sink, mut outbox) = outbox(
            Framing::Newline,
            QueueLimits::new().with_write_timeout(Some(Duration::from_millis(10))),
        );
        assert!(sink.send("1").is_ok());
        assert!(outbox.poll_chunk(&mut cx).is_ready()); // taken, never consumed
        thread::sleep(Duration::from_millis(30));
        let error = sink.send("2").err().map(|e| e.kind());
        assert_eq!(error, Some(io::ErrorKind::TimedOut));
        assert_eq!(outbox.poll_chunk(&mut cx), Poll::Ready(&b"1\n"[..]));
        outbox.consume(2);
        assert_eq!(outbox.poll_chunk(&mut cx), Poll::Ready(&b""[..]));
    }

    #[test]
    fn test_dropped_outbox_fails_the_sink() {
        let (mut sink, outbox) = outbox(Framing::Newline, QueueLimits::new());
        drop(outbox);
        assert_eq!(
            sink.send("x").err().map(|e| e.kind()),
            Some(io::ErrorKind::BrokenPipe)
        );
        assert!(sink.close().is_err());
    }

    #[test]
    fn test_outbox_partial_consume_and_recycling() {
        let mut cx = Context::from_waker(Waker::noop());
        let (mut sink, mut outbox) = outbox(Framing::Newline, QueueLimits::new());
        let big = "x".repeat(4 * KEEP);
        assert!(sink.send(&big).is_ok());
        let Poll::Ready(chunk) = outbox.poll_chunk(&mut cx) else {
            panic!("nothing to write")
        };
        assert_eq!(chunk.len(), 4 * KEEP + 1);
        outbox.consume(10);
        let Poll::Ready(rest) = outbox.poll_chunk(&mut cx) else {
            panic!("the rest is gone")
        };
        assert_eq!(rest.len(), 4 * KEEP + 1 - 10);
        outbox.consume(usize::MAX);
        assert!(outbox.poll_chunk(&mut cx).is_pending());
        assert!(outbox.batch.capacity() <= 2 * KEEP);
        assert_eq!(outbox.pending_bytes(), 0);
        assert!(format!("{outbox:?} {sink:?}").contains("pending_bytes: 0"));
    }

    #[test]
    fn test_queued_writes_in_order_off_the_sender_thread() {
        let (go, release) = mpsc::channel();
        let (out, written) = mpsc::channel();
        let mut sink = match Queued::new(Gate { go: release, out }, Framing::Newline) {
            Ok(sink) => sink,
            Err(error) => panic!("{error}"),
        };
        // The writer blocks on its first write; sends still return at once.
        let started = Instant::now();
        for i in 0..100 {
            assert!(sink.send(&i.to_string()).is_ok());
        }
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(sink.pending_messages() > 0);
        for _ in 0..100 {
            let _go = go.send(());
        }
        let mut text = Vec::new();
        while text.len() < 290 {
            match written.recv_timeout(Duration::from_secs(5)) {
                Ok(chunk) => text.extend(chunk),
                Err(_) => panic!("the writer stopped"),
            }
        }
        let expected: Vec<u8> = (0..100)
            .flat_map(|i| format!("{i}\n").into_bytes())
            .collect();
        assert_eq!(text, expected);
        drop(go);
        assert!(sink.close().is_ok());
        assert!(format!("{sink:?}").starts_with("Queued"));
    }

    #[test]
    fn test_queued_reports_a_failed_write_and_a_stall() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::Error::new(io::ErrorKind::BrokenPipe, "gone"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut sink = match Queued::new(Broken, Framing::Newline) {
            Ok(sink) => sink,
            Err(error) => panic!("{error}"),
        };
        assert!(sink.send("x").is_ok());
        let deadline = Instant::now() + Duration::from_secs(5);
        while sink.flush().is_ok() {
            assert!(Instant::now() < deadline, "the failure was not reported");
            thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            sink.send("y").err().map(|e| e.kind()),
            Some(io::ErrorKind::BrokenPipe)
        );

        // A writer stuck in a write: the sender times out, and closing gives
        // up on it instead of waiting forever.
        let (_go, release) = mpsc::channel();
        let (out, _written) = mpsc::channel();
        let limits = QueueLimits::new().with_write_timeout(Some(Duration::from_millis(20)));
        let mut sink =
            match Queued::with_limits(Gate { go: release, out }, Framing::Newline, limits) {
                Ok(sink) => sink,
                Err(error) => panic!("{error}"),
            };
        assert!(sink.send("x").is_ok());
        thread::sleep(Duration::from_millis(60));
        assert_eq!(
            sink.send("y").err().map(|e| e.kind()),
            Some(io::ErrorKind::TimedOut)
        );
        let started = Instant::now();
        assert!(sink.close().is_err());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn test_queued_contains_a_panicking_writer() {
        struct Panics;
        impl Write for Panics {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                panic!("deliberate: writer")
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let mut sink = match Queued::new(Panics, Framing::Newline) {
            Ok(sink) => sink,
            Err(error) => panic!("{error}"),
        };
        assert!(sink.send("x").is_ok());
        let deadline = Instant::now() + Duration::from_secs(5);
        while sink.flush().is_ok() {
            assert!(Instant::now() < deadline, "the panic was not reported");
            thread::sleep(Duration::from_millis(2));
        }
        let error = sink.flush().err().map(|e| e.to_string());
        assert_eq!(
            error.as_deref(),
            Some("writing to the peer failed: the writer panicked")
        );
    }
}
