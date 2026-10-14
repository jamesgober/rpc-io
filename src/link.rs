//! The state an endpoint shares with its handles ([`Peer`](crate::Peer),
//! [`Call`](crate::Call), [`Responder`](crate::Responder)): the sink, our
//! outstanding requests, the peer's deferred requests, open batches, and the
//! gate.
//!
//! Everything sits behind one mutex. Handles on other threads send through it
//! directly, so a deferred reply or a notification from a worker goes out at
//! once, even while the endpoint's own thread is blocked reading. The lock is
//! held to encode a message and hand it to the sink, never while a handler or
//! event hook runs. One connection's traffic is serial on the wire anyway, so
//! a single lock costs nothing a lock-free design would save, as long as the
//! sink does not block: a sink that writes to a slow peer holds the lock for
//! the write, which is why [`Queued`](crate::Queued) exists.
//!
//! Deadlines fire on their own: the first request with one starts a timer
//! thread that sleeps until the earliest is due, expires it, and sleeps again.
//! It holds only a weak reference, so it never keeps the connection alive,
//! and it stops when the connection closes.

use std::{
    collections::{BTreeMap, BTreeSet},
    io, mem,
    panic::{self, AssertUnwindSafe},
    sync::{
        Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, ThreadId},
    time::{Duration, Instant},
};

use crate::{
    config::{Cancellation, Config},
    error::{CallError, Error},
    error_object::ErrorObject,
    handles::CancelToken,
    id::Id,
    message::{write_call, write_numbered_call, write_response},
    pending::Pending,
    raw::Json,
    sink::{Drain, Sink},
    slot::{self, Outcome, Slot, Waiter},
    stats::{Stats, Tally, bump},
    text,
    trace::event,
};

/// The encoding buffer is released after a message that grew it past this.
const SCRATCH_KEEP: usize = 64 * 1024;

/// What the handles need from the shared state, with the sink type erased.
pub(crate) trait Link: Send + Sync {
    fn notify(&self, method: &str, params: Option<&Json<'_>>) -> Result<(), Error>;
    fn request(
        &self,
        method: &str,
        params: Option<&Json<'_>>,
        timeout: Option<Duration>,
    ) -> Result<Issued, Error>;
    fn cancel(&self, id: u64) -> bool;
    fn expire(&self);
    fn respond(
        &self,
        id: &Id<'static>,
        result: Result<&Json<'_>, &ErrorObject>,
    ) -> Result<(), Error>;
    fn on_endpoint_thread(&self) -> bool;
    fn is_closed(&self) -> bool;
    fn stats(&self) -> Stats;
}

/// A request just sent: its id, where its outcome arrives, its deadline.
pub(crate) struct Issued {
    pub(crate) id: u64,
    pub(crate) outcome: Arc<Slot>,
    pub(crate) deadline: Option<Instant>,
}

pub(crate) struct Shared<K> {
    inner: Mutex<Inner<K>>,
    timer: Arc<Timer>,
    /// `Inner::closed`, readable without the lock.
    closed: Arc<AtomicBool>,
}

impl<K: Sink> Shared<K> {
    pub(crate) fn new(sink: K, config: &Config) -> Arc<Self> {
        let timer = Arc::new(Timer::default());
        let closed = Arc::new(AtomicBool::new(false));
        Arc::new_cyclic(|me| Self {
            timer: Arc::clone(&timer),
            closed: Arc::clone(&closed),
            inner: Mutex::new(Inner {
                me: Weak::clone(me),
                timer,
                closed_flag: closed,
                sink,
                sink_closed: false,
                closed: false,
                broken: None,
                corked: false,
                scratch: String::new(),
                pending: Pending::new(config.max_pending),
                in_flight: IdMap::default(),
                batches: BTreeMap::new(),
                next_batch: 0,
                gate: if config.gate.is_some() {
                    GateState::Closed
                } else {
                    GateState::Open
                },
                held: Vec::new(),
                held_bytes: 0,
                held_requests: BTreeSet::new(),
                stats: Stats::default(),
                settings: Settings::from(config),
                epoch: Instant::now(),
                endpoint_thread: None,
            }),
        })
    }

    /// The state. A panic while it was locked can only have come from a
    /// [`Sink`]; the state is still usable, so the poison is ignored.
    pub(crate) fn lock(&self) -> MutexGuard<'_, Inner<K>> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether the connection is closed, without taking the lock: a sender
    /// stuck in a slow sink does not hold this up.
    pub(crate) fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

impl<K> Drop for Shared<K> {
    fn drop(&mut self) {
        self.timer.stop();
    }
}

impl<K: Sink> Link for Shared<K> {
    fn notify(&self, method: &str, params: Option<&Json<'_>>) -> Result<(), Error> {
        self.lock().notify(method, params)
    }

    fn request(
        &self,
        method: &str,
        params: Option<&Json<'_>>,
        timeout: Option<Duration>,
    ) -> Result<Issued, Error> {
        self.lock().request(method, params, timeout)
    }

    fn cancel(&self, id: u64) -> bool {
        self.lock().cancel_outgoing(id)
    }

    fn expire(&self) {
        self.lock().expire();
    }

    fn respond(
        &self,
        id: &Id<'static>,
        result: Result<&Json<'_>, &ErrorObject>,
    ) -> Result<(), Error> {
        self.lock().respond_deferred(id, result)
    }

    fn on_endpoint_thread(&self) -> bool {
        self.lock().endpoint_thread == Some(thread::current().id())
    }

    fn is_closed(&self) -> bool {
        Shared::is_closed(self)
    }

    fn stats(&self) -> Stats {
        self.lock().stats()
    }
}

/// The settings the shared state applies.
pub(crate) struct Settings {
    pub(crate) cancellation: Option<Cancellation>,
    pub(crate) request_timeout: Option<Duration>,
    pub(crate) max_in_flight: usize,
    pub(crate) max_held_bytes: usize,
    pub(crate) timer: bool,
}

impl From<&Config> for Settings {
    fn from(config: &Config) -> Self {
        Self {
            cancellation: config.cancellation.clone(),
            request_timeout: config.request_timeout,
            max_in_flight: config.max_in_flight,
            max_held_bytes: config.max_held_bytes,
            timer: config.timer,
        }
    }
}

/// When the timer thread should next wake, and whether it runs.
#[derive(Default)]
struct Timer {
    state: Mutex<TimerState>,
    wake: Condvar,
}

#[derive(Default)]
struct TimerState {
    /// The earliest deadline it knows of (it may have been settled since;
    /// waking for nothing is harmless).
    due: Option<Instant>,
    running: bool,
    stopped: bool,
}

impl Timer {
    fn lock(&self) -> MutexGuard<'_, TimerState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Ends the timer thread, for good.
    fn stop(&self) {
        self.lock().stopped = true;
        self.wake.notify_all();
    }
}

/// The timer thread: sleeps until the earliest deadline, expires what is due,
/// and asks the state for the next one. Ends when the timer is stopped or the
/// shared state is gone.
fn run_timer<K: Sink>(timer: &Timer, shared: &Weak<Shared<K>>) {
    let mut state = timer.lock();
    while !state.stopped {
        let Some(due) = state.due else {
            state = timer
                .wake
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
            continue;
        };
        let now = Instant::now();
        if due > now {
            state = timer
                .wake
                .wait_timeout(state, due - now)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
            continue;
        }
        state.due = None;
        drop(state);
        let Some(shared) = shared.upgrade() else {
            return;
        };
        {
            let mut inner = shared.lock();
            inner.expire();
            inner.arm();
        }
        drop(shared);
        state = timer.lock();
    }
    state.running = false;
}

/// Where the gate is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum GateState {
    /// No gate, or the opening request succeeded.
    Open,
    /// Waiting for the opening request.
    Closed,
    /// The opening request with this id is being answered.
    Opening(Id<'static>),
}

/// A peer request answered later through a [`Responder`](crate::Responder).
struct InFlight {
    token: CancelToken,
    batch: Option<u64>,
}

/// Deferred requests by id. Integer and string ids are kept apart, so a
/// lookup borrows the id it is given (`&str` for a string) instead of copying
/// it into an owned key.
struct IdMap<V> {
    numbers: BTreeMap<i64, V>,
    strings: BTreeMap<Box<str>, V>,
}

impl<V> Default for IdMap<V> {
    fn default() -> Self {
        Self {
            numbers: BTreeMap::new(),
            strings: BTreeMap::new(),
        }
    }
}

impl<V> IdMap<V> {
    fn len(&self) -> usize {
        self.numbers.len() + self.strings.len()
    }

    fn contains(&self, id: &Id<'_>) -> bool {
        match id {
            Id::Number(n) => self.numbers.contains_key(n),
            Id::String(s) => self.strings.contains_key(&**s),
        }
    }

    fn insert(&mut self, id: Id<'_>, value: V) -> Option<V> {
        match id {
            Id::Number(n) => self.numbers.insert(n, value),
            Id::String(s) => self.strings.insert(s.into_owned().into_boxed_str(), value),
        }
    }

    fn remove(&mut self, id: &Id<'_>) -> Option<V> {
        match id {
            Id::Number(n) => self.numbers.remove(n),
            Id::String(s) => self.strings.remove(&**s),
        }
    }

    fn into_values(self) -> impl Iterator<Item = V> {
        self.numbers.into_values().chain(self.strings.into_values())
    }
}

/// A batch being answered: responses so far, and how many are still owed.
struct Batch {
    remaining: usize,
    body: String,
}

/// A message held back until the gate opens.
struct Held {
    request: Option<u64>,
    body: String,
}

/// What a message is, for the gate.
#[derive(Clone, Copy)]
enum Sending {
    Response,
    Notification,
    Request(u64),
}

pub(crate) struct Inner<K> {
    /// The state this sits in, for the timer thread.
    me: Weak<Shared<K>>,
    timer: Arc<Timer>,
    /// Mirrors `closed` for `Shared::is_closed`.
    closed_flag: Arc<AtomicBool>,
    sink: K,
    /// Whether `Sink::close` was called.
    sink_closed: bool,
    closed: bool,
    /// The first failure of the sink, for the endpoint's thread to report.
    broken: Option<io::Error>,
    /// While set (the endpoint is handling a message), the sink is flushed at
    /// the end of the turn instead of after each message.
    corked: bool,
    /// Reused buffer every message is encoded in.
    scratch: String,
    pending: Pending<Waiter>,
    in_flight: IdMap<InFlight>,
    batches: BTreeMap<u64, Batch>,
    next_batch: u64,
    pub(crate) gate: GateState,
    held: Vec<Held>,
    /// The bytes of the bodies in `held`.
    held_bytes: usize,
    /// Ids of our requests in `held` and not abandoned since: lookups stay
    /// `O(log n)` however much is held.
    held_requests: BTreeSet<u64>,
    /// The counters; the gauges are filled in by `stats`.
    stats: Stats,
    pub(crate) settings: Settings,
    epoch: Instant,
    /// The thread inside the endpoint's `handle` or `run`, if any.
    pub(crate) endpoint_thread: Option<ThreadId>,
}

impl<K: Sink> Inner<K> {
    pub(crate) fn sink_mut(&mut self) -> &mut K {
        &mut self.sink
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.closed
    }

    /// A snapshot of the counters, with the gauges as they are now.
    pub(crate) fn stats(&self) -> Stats {
        Stats {
            pending: self.pending.len(),
            in_flight: self.in_flight.len(),
            held: self.held.len(),
            ..self.stats
        }
    }

    /// Counts an incoming body (or, with `tally.body` unset, a message the
    /// framing skipped).
    pub(crate) fn take_in(&mut self, tally: &Tally) {
        self.stats.take_in(tally);
    }

    /// Counts a contained handler panic.
    pub(crate) fn count_panic(&mut self) {
        bump(&mut self.stats.panics);
    }

    /// Replaces the settings and the capacity of the pending map (kept if
    /// requests are already outstanding).
    pub(crate) fn configure(&mut self, config: &Config) {
        self.settings = Settings::from(config);
        if self.pending.is_empty() {
            self.pending = Pending::new(config.max_pending);
        }
        if config.gate.is_none() {
            self.gate = GateState::Open;
        } else if self.gate == GateState::Open && self.held.is_empty() {
            self.gate = GateState::Closed;
        }
    }

    fn now(&self) -> Duration {
        self.epoch.elapsed()
    }

    /// When the earliest outstanding request is due.
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.pending
            .next_deadline()
            .and_then(|at| self.epoch.checked_add(at))
    }

    /// Makes sure the timer thread wakes for the earliest deadline, starting
    /// it if it is not running. Without a thread (the timer is off, or one
    /// could not be started) deadlines are enforced lazily, as `expire` says.
    fn arm(&mut self) {
        if !self.settings.timer || self.closed {
            return;
        }
        let Some(due) = self.next_deadline() else {
            return;
        };
        let mut timer = self.timer.lock();
        if timer.stopped {
            return;
        }
        if timer.due.is_none_or(|at| due < at) {
            timer.due = Some(due);
            self.timer.wake.notify_all();
        }
        if !timer.running {
            let shared = Weak::clone(&self.me);
            let thread_timer = Arc::clone(&self.timer);
            let started = thread::Builder::new()
                .name("rpc-io-timer".into())
                .spawn(move || run_timer(&thread_timer, &shared));
            timer.running = started.is_ok();
        }
    }

    pub(crate) fn cork(&mut self) {
        self.corked = true;
    }

    /// Whether the calling thread is the one inside the endpoint's turn.
    fn on_endpoint_thread(&self) -> bool {
        self.endpoint_thread == Some(thread::current().id())
    }

    /// Ends a turn: everything sent during it goes out.
    pub(crate) fn uncork(&mut self) {
        self.corked = false;
        if !self.closed {
            if let Err(error) = guard(|| self.sink.flush()) {
                let _closed = self.fail(error);
            }
        }
    }

    /// The sink's first failure, once.
    pub(crate) fn take_broken(&mut self) -> Option<io::Error> {
        self.broken.take()
    }

    /// Closes the connection: closes the sink (flushing it), tells every
    /// waiter it is over, and returns what the caller waits for once it has
    /// released the lock (the sink's [`Drain`]).
    pub(crate) fn close(&mut self) -> Option<Drain> {
        if self.closed {
            return None;
        }
        if let Err(error) = self.close_sink() {
            let _closed = self.fail(error);
        }
        self.shut();
        let drain = panic::catch_unwind(AssertUnwindSafe(|| self.sink.drain()));
        drain.unwrap_or(None)
    }

    /// Closes the sink, once: at the close, or after a send failed.
    fn close_sink(&mut self) -> io::Result<()> {
        if mem::replace(&mut self.sink_closed, true) {
            return Ok(());
        }
        guard(|| self.sink.close())
    }

    /// Marks the connection closed and releases everything waiting on it.
    fn shut(&mut self) {
        event!(debug, "the connection is closed");
        self.closed = true;
        self.closed_flag.store(true, Ordering::Release);
        self.timer.stop();
        for (_, waiter) in self.pending.drain() {
            waiter.send(Err(CallError::Closed));
        }
        for request in mem::take(&mut self.in_flight).into_values() {
            // Workers still answering see the cancellation and can stop.
            request.token.cancel();
        }
        self.batches.clear();
        self.held.clear();
        self.held_requests.clear();
        self.held_bytes = 0;
    }

    /// Records a sink failure and closes; returns the error for the sender.
    fn fail(&mut self, error: io::Error) -> Error {
        event!(warn, error = %error, "the transport failed; closing the connection");
        let copy = io::Error::new(error.kind(), error.to_string());
        if self.broken.is_none() {
            self.broken = Some(error);
        }
        // Best effort: the sink failed already, but it still gets to release
        // what it holds.
        let _released = self.close_sink();
        self.shut();
        Error::Io(copy)
    }

    /// Sends the message in `scratch`, or holds it while the gate is shut.
    fn deliver(&mut self, sending: Sending) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        if self.gate != GateState::Open && !matches!(sending, Sending::Response) {
            let bytes = self.held_bytes.saturating_add(self.scratch.len());
            if bytes > self.settings.max_held_bytes {
                event!(
                    warn,
                    held = self.held.len(),
                    "too much is held behind the gate; a message was refused"
                );
                return Err(Error::Backlog);
            }
            self.held_bytes = bytes;
            let request = match sending {
                Sending::Request(id) => Some(id),
                _ => None,
            };
            if let Some(id) = request {
                let _fresh = self.held_requests.insert(id);
            }
            self.held.push(Held {
                request,
                body: self.scratch.clone(),
            });
            return Ok(());
        }
        bump(&mut self.stats.bodies_out);
        self.stats.bytes_out = self.stats.bytes_out.wrapping_add(self.scratch.len() as u64);
        match sending {
            Sending::Request(_) => bump(&mut self.stats.requests_out),
            Sending::Notification => bump(&mut self.stats.notifications_out),
            // Counted one by one in `respond`, batch elements included.
            Sending::Response => {}
        }
        let mut sent = guard(|| self.sink.send(&self.scratch));
        // Inside a turn the endpoint flushes once at its end; a message from
        // any other thread (a worker answering, a progress report) must not
        // wait for a handler that may run for seconds.
        if sent.is_ok() && (!self.corked || !self.on_endpoint_thread()) {
            sent = guard(|| self.sink.flush());
        }
        if self.scratch.capacity() > SCRATCH_KEEP {
            // A large message or batch grew the buffer; keep the usual size.
            self.scratch = String::new();
        }
        sent.map_err(|error| self.fail(error))
    }

    pub(crate) fn notify(&mut self, method: &str, params: Option<&Json<'_>>) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.scratch.clear();
        write_call(&mut self.scratch, None, method, params);
        self.deliver(Sending::Notification)
    }

    pub(crate) fn request(
        &mut self,
        method: &str,
        params: Option<&Json<'_>>,
        timeout: Option<Duration>,
    ) -> Result<Issued, Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        self.expire();
        let (waiter, outcome) = slot::slot();
        let deadline = timeout
            .or(self.settings.request_timeout)
            .and_then(|t| self.now().checked_add(t));
        let (id, evicted) = self.pending.issue(waiter, deadline);
        if let Some((oldest, waiter)) = evicted {
            self.abandon(oldest, waiter, CallError::Evicted);
        }
        self.scratch.clear();
        write_numbered_call(&mut self.scratch, id, method, params);
        if let Err(error) = self.deliver(Sending::Request(id)) {
            let _unsent = self.pending.settle(id);
            return Err(error);
        }
        if deadline.is_some() {
            self.arm();
        }
        Ok(Issued {
            id,
            outcome,
            deadline: deadline.and_then(|at| self.epoch.checked_add(at)),
        })
    }

    /// Ends every request whose deadline has passed with `TimedOut`.
    pub(crate) fn expire(&mut self) {
        let now = self.now();
        while let Some((id, waiter)) = self.pending.pop_expired(now) {
            self.abandon(id, waiter, CallError::TimedOut);
        }
    }

    pub(crate) fn cancel_outgoing(&mut self, id: u64) -> bool {
        match self.pending.settle(id) {
            Some(waiter) => {
                self.abandon(id, waiter, CallError::Cancelled);
                true
            }
            None => false,
        }
    }

    /// Delivers the answer to request `id`; `false` if it is not outstanding,
    /// or is still held behind the gate (the peer has never seen it, so the
    /// "answer" is forged or confused).
    pub(crate) fn settle(&mut self, id: u64, outcome: Outcome) -> bool {
        if self.held_requests.contains(&id) {
            return false;
        }
        match self.pending.settle(id) {
            Some(waiter) => {
                waiter.send(outcome);
                true
            }
            None => false,
        }
    }

    /// Ends a request removed from `pending` without an answer: tells the peer
    /// to stop (unless it was never sent), and tells the waiter why.
    fn abandon(&mut self, id: u64, waiter: Waiter, reason: CallError) {
        match reason {
            CallError::TimedOut => {
                bump(&mut self.stats.timed_out);
                event!(debug, id, "our request timed out");
            }
            CallError::Evicted => {
                bump(&mut self.stats.evicted);
                event!(
                    warn,
                    id,
                    "our oldest request was evicted: too many are outstanding"
                );
            }
            _ => bump(&mut self.stats.cancelled_out),
        }
        // Never sent: its held body is skipped when the gate opens, and the
        // peer has nothing to cancel.
        if !self.held_requests.remove(&id) && !self.closed {
            self.send_cancel(id);
        }
        waiter.send(Err(reason));
    }

    /// Sends the cancel notification for our request `id`. Best effort: if
    /// the transport has failed, the request is moot anyway.
    fn send_cancel(&mut self, id: u64) {
        let Some(cancellation) = &self.settings.cancellation else {
            return;
        };
        self.scratch.clear();
        self.scratch.push_str("{\"jsonrpc\":\"2.0\",\"method\":");
        jsonx_lib::write_string(&mut self.scratch, &cancellation.method);
        self.scratch.push_str(",\"params\":{");
        jsonx_lib::write_string(&mut self.scratch, &cancellation.id_member);
        self.scratch.push(':');
        text::push_u64(&mut self.scratch, id);
        self.scratch.push_str("}}");
        let _best_effort = self.deliver(Sending::Notification);
    }

    /// Answers a peer request (or a malformed message) now.
    pub(crate) fn respond(
        &mut self,
        id: Option<&Id<'_>>,
        batch: Option<u64>,
        result: Result<&Json<'_>, &ErrorObject>,
    ) -> Result<(), Error> {
        if self.closed {
            return Err(Error::Closed);
        }
        bump(&mut self.stats.responses_out);
        if result.is_err() {
            bump(&mut self.stats.errors_out);
        }
        self.scratch.clear();
        write_response(&mut self.scratch, id, result);
        let opening = match (&self.gate, id) {
            (GateState::Opening(opening), Some(id)) => opening == id,
            _ => false,
        };
        let sent = match batch {
            Some(batch) => self.add_to_batch(batch, true),
            None => self.deliver(Sending::Response),
        };
        if opening {
            self.settle_gate(result.is_ok());
        }
        sent
    }

    /// Adds the response in `scratch` (or, for a silently cancelled request,
    /// nothing) to `batch`; sends the batch once nothing more is owed.
    fn add_to_batch(&mut self, batch: u64, with_response: bool) -> Result<(), Error> {
        let Some(open) = self.batches.get_mut(&batch) else {
            return Ok(());
        };
        if with_response {
            if open.body.len() > 1 {
                open.body.push(',');
            }
            open.body.push_str(&self.scratch);
        }
        open.remaining = open.remaining.saturating_sub(1);
        if open.remaining > 0 {
            return Ok(());
        }
        let Some(mut done) = self.batches.remove(&batch) else {
            return Ok(());
        };
        if done.body.len() == 1 {
            // Every request in it was cancelled without a reply.
            return Ok(());
        }
        done.body.push(']');
        mem::swap(&mut self.scratch, &mut done.body);
        self.deliver(Sending::Response)
    }

    /// Opens the gate after a successful opening response, sending what was
    /// held; or shuts it again after a failed one, discarding what was held.
    fn settle_gate(&mut self, succeeded: bool) {
        let held = mem::take(&mut self.held);
        let live = mem::take(&mut self.held_requests);
        self.held_bytes = 0;
        if succeeded {
            self.gate = GateState::Open;
            for message in held {
                if message.request.is_some_and(|id| !live.contains(&id)) {
                    continue;
                }
                self.scratch = message.body;
                let sending = message
                    .request
                    .map_or(Sending::Notification, Sending::Request);
                if self.deliver(sending).is_err() {
                    break;
                }
            }
        } else {
            self.gate = GateState::Closed;
            for message in held {
                if let Some(id) = message.request.filter(|id| live.contains(id)) {
                    if let Some(waiter) = self.pending.settle(id) {
                        waiter.send(Err(CallError::Cancelled));
                    }
                }
            }
        }
    }

    /// Opens a batch that owes `answers` responses.
    pub(crate) fn open_batch(&mut self, answers: usize) -> u64 {
        let id = self.next_batch;
        self.next_batch = self.next_batch.wrapping_add(1);
        let _fresh = self.batches.insert(
            id,
            Batch {
                remaining: answers,
                body: String::from("["),
            },
        );
        id
    }

    /// Whether a deferred request with this id is being answered.
    pub(crate) fn in_flight(&self, id: &Id<'_>) -> bool {
        self.in_flight.contains(id)
    }

    /// Records a deferred request; `None` when too many are in flight.
    pub(crate) fn begin_deferred(
        &mut self,
        id: Id<'static>,
        batch: Option<u64>,
    ) -> Option<CancelToken> {
        if self.in_flight.len() >= self.settings.max_in_flight {
            return None;
        }
        let token = CancelToken::new();
        let _fresh = self.in_flight.insert(
            id,
            InFlight {
                token: token.clone(),
                batch,
            },
        );
        Some(token)
    }

    /// Answers a deferred request, if it is still in flight.
    pub(crate) fn respond_deferred(
        &mut self,
        id: &Id<'static>,
        result: Result<&Json<'_>, &ErrorObject>,
    ) -> Result<(), Error> {
        match self.in_flight.remove(id) {
            Some(request) => self.respond(Some(id), request.batch, result),
            None if self.closed => Err(Error::Closed),
            None => Err(Error::AlreadyAnswered),
        }
    }

    /// The peer cancelled its request `id`: sets its token and answers it as
    /// the cancellation settings say. `false` if it is not in flight.
    pub(crate) fn cancel_incoming(&mut self, id: &Id<'_>) -> bool {
        let Some(request) = self.in_flight.remove(id) else {
            return false;
        };
        bump(&mut self.stats.cancelled_in);
        request.token.cancel();
        let reply = self.settings.cancellation.as_ref().and_then(|c| c.reply);
        let _sent = match reply {
            Some(code) => {
                let error = ErrorObject::new(code, "the request was cancelled");
                self.respond(Some(id), request.batch, Err(&error))
            }
            None => {
                // No response will come to settle the gate: a cancelled
                // opening request has failed, so the gate shuts again.
                if matches!(&self.gate, GateState::Opening(opening) if opening == id) {
                    self.settle_gate(false);
                }
                match request.batch {
                    Some(batch) => self.add_to_batch(batch, false),
                    None => Ok(()),
                }
            }
        };
        true
    }
}

/// Runs a [`Sink`] call, turning a panic in it into an I/O error: the sink is
/// user code, and its panic must close the connection, not unwind through the
/// endpoint (or through a worker that happened to be sending).
fn guard(send: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    panic::catch_unwind(AssertUnwindSafe(send))
        .unwrap_or_else(|_| Err(io::Error::other("the sink panicked")))
}
