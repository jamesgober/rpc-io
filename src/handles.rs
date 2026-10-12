//! Handles onto a running endpoint: [`Peer`] to send, [`Call`] to await an
//! answer, [`Canceller`] to cancel one, [`Responder`] to answer a deferred
//! request, and [`CancelToken`] to learn that the peer cancelled it.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, RecvTimeoutError, TryRecvError},
    },
    thread,
    time::{Duration, Instant},
};

use jsonx_lib::Kind;

use crate::{
    error::{CallError, Error},
    error_object::ErrorObject,
    id::Id,
    link::{Link, Outcome},
    raw::{Json, Raw},
};

/// A flag that says a request was cancelled.
///
/// A [`Responder`] carries one: the endpoint sets it when the peer cancels
/// the request (or the connection closes), and the code answering the request
/// checks it to stop early. Clones share the flag. Setting it by hand is
/// allowed and only sets the flag.
///
/// # Examples
///
/// ```
/// let token = rpc_io::CancelToken::new();
/// let seen_by_worker = token.clone();
/// assert!(!seen_by_worker.is_cancelled());
/// token.cancel();
/// assert!(seen_by_worker.is_cancelled());
/// ```
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// A token that is not cancelled.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(!rpc_io::CancelToken::new().is_cancelled());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the flag, for this token and every clone.
    ///
    /// # Examples
    ///
    /// ```
    /// let t = rpc_io::CancelToken::new();
    /// t.cancel();
    /// t.cancel(); // idempotent
    /// assert!(t.is_cancelled());
    /// ```
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Whether the flag is set.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(!rpc_io::CancelToken::new().is_cancelled());
    /// ```
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Outgoing params: `null` means none; arrays and objects are sent; anything
/// else is refused.
fn outgoing_params<'a>(params: impl Into<Json<'a>>) -> Result<Option<Json<'a>>, Error> {
    let params = params.into();
    match params.kind() {
        Kind::Null => Ok(None),
        Kind::Array | Kind::Object => Ok(Some(params)),
        kind => Err(Error::InvalidParams(kind)),
    }
}

/// A handle for talking to the peer: notifications and requests, from any
/// thread.
///
/// Cheap to clone, `Send` and `Sync`. Messages sent from a thread other than
/// the endpoint's go out at once (one write and flush each); messages sent
/// from a handler go out with the rest of that turn, before the handler's
/// response.
///
/// # Examples
///
/// ```
/// use std::sync::mpsc;
/// use jsonx_lib::json;
/// use rpc_io::{Endpoint, Router};
///
/// let (tx, rx) = mpsc::channel::<String>();
/// let endpoint = Endpoint::new(tx, Router::new(), ());
/// let peer = endpoint.peer();
/// std::thread::spawn(move || peer.notify("progress", json!({ "done": 50 })))
///     .join()
///     .unwrap()?;
/// assert_eq!(
///     rx.try_recv().ok().as_deref(),
///     Some(r#"{"jsonrpc":"2.0","method":"progress","params":{"done":50}}"#),
/// );
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone)]
pub struct Peer {
    link: Arc<dyn Link>,
}

impl Peer {
    pub(crate) fn new(link: Arc<dyn Link>) -> Self {
        Self { link }
    }

    /// Sends a notification. `params` may be an array, an object, or `()`
    /// (`null`) for none.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidParams`] for other params; [`Error::Closed`] once the
    /// connection has ended; [`Error::Io`] if writing failed (which closes the
    /// connection).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Error, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// assert!(peer.notify("exit", ()).is_ok());
    /// assert!(matches!(peer.notify("bad", 5), Err(Error::InvalidParams(_))));
    /// ```
    pub fn notify<'a>(&self, method: &str, params: impl Into<Json<'a>>) -> Result<(), Error> {
        let params = outgoing_params(params)?;
        self.link.notify(method, params.as_ref())
    }

    /// Sends a request and returns the [`Call`] its answer will arrive on. The
    /// deadline is [`Config::request_timeout`](crate::Config), if set.
    ///
    /// # Errors
    ///
    /// As [`notify`](Peer::notify).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let call = peer.request("workspace/configuration", jsonx_lib::json!({ "items": [] }))?;
    /// assert_eq!(call.id(), 1);
    /// assert!(rx.try_recv().is_ok_and(|m| m.starts_with(r#"{"jsonrpc":"2.0","id":1,"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn request<'a>(&self, method: &str, params: impl Into<Json<'a>>) -> Result<Call, Error> {
        self.send_request(method, params, None)
    }

    /// Sends a request that times out after `timeout`: its [`Call`] then ends
    /// with [`CallError::TimedOut`] and the peer is sent the cancel
    /// notification.
    ///
    /// # Errors
    ///
    /// As [`notify`](Peer::notify).
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use rpc_io::{CallError, Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let call = peer.request_with_timeout("slow", (), Duration::from_millis(10))?;
    /// assert_eq!(call.wait(), Err(CallError::TimedOut));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn request_with_timeout<'a>(
        &self,
        method: &str,
        params: impl Into<Json<'a>>,
        timeout: Duration,
    ) -> Result<Call, Error> {
        self.send_request(method, params, Some(timeout))
    }

    fn send_request<'a>(
        &self,
        method: &str,
        params: impl Into<Json<'a>>,
        timeout: Option<Duration>,
    ) -> Result<Call, Error> {
        let params = outgoing_params(params)?;
        let issued = self.link.request(method, params.as_ref(), timeout)?;
        Ok(Call {
            id: issued.id,
            outcome: issued.outcome,
            deadline: issued.deadline,
            link: Arc::clone(&self.link),
        })
    }

    /// Whether the connection has ended.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ());
    /// let peer = endpoint.peer();
    /// endpoint.close();
    /// assert!(peer.is_closed());
    /// ```
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.link.is_closed()
    }
}

impl fmt::Debug for Peer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peer")
            .field("closed", &self.is_closed())
            .finish()
    }
}

/// One of our requests, awaiting its answer.
///
/// Ends in exactly one outcome: the peer's result, or a [`CallError`]
/// (rejected, timed out, cancelled, evicted, connection closed). Dropping a
/// `Call` does not cancel the request; its answer is then discarded.
///
/// # Examples
///
/// ```
/// use rpc_io::{CallError, Endpoint, Router};
///
/// let (tx, _rx) = std::sync::mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, Router::new(), ());
/// let call = endpoint.peer().request("x", ())?;
/// // The peer answers through the endpoint:
/// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[42]}"#)?;
/// assert_eq!(call.wait().map(|raw| raw.as_str().to_owned()), Ok("[42]".to_owned()));
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Call {
    id: u64,
    outcome: mpsc::Receiver<Outcome>,
    deadline: Option<Instant>,
    link: Arc<dyn Link>,
}

impl Call {
    /// The request's id on the wire.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// assert_eq!(peer.request("a", ())?.id(), 1);
    /// assert_eq!(peer.request("b", ())?.id(), 2);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[inline]
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }

    /// The deadline, if the request has one.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// assert!(peer.request("a", ())?.deadline().is_none());
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[inline]
    #[must_use]
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Blocks until the answer arrives or the request ends otherwise.
    ///
    /// # Errors
    ///
    /// The [`CallError`] the request ended with. On the endpoint's own thread
    /// (inside a handler) it returns [`CallError::WouldDeadlock`] at once
    /// unless the outcome is already there: that thread is the one that would
    /// read the answer.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{CallError, Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ());
    /// let call = endpoint.peer().request("x", ())?;
    /// endpoint.close();
    /// assert_eq!(call.wait(), Err(CallError::Closed));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn wait(self) -> Result<Raw<'static>, CallError> {
        match self.outcome.try_recv() {
            Ok(outcome) => return outcome,
            Err(TryRecvError::Disconnected) => return Err(CallError::Closed),
            Err(TryRecvError::Empty) => {}
        }
        if self.link.on_endpoint_thread() {
            return Err(CallError::WouldDeadlock);
        }
        if let Some(deadline) = self.deadline {
            let left = deadline.saturating_duration_since(Instant::now());
            match self.outcome.recv_timeout(left) {
                Ok(outcome) => return outcome,
                Err(RecvTimeoutError::Disconnected) => return Err(CallError::Closed),
                // Due: expiring delivers `TimedOut` (or the answer, if it won
                // the race), so the blocking receive below returns at once.
                Err(RecvTimeoutError::Timeout) => self.link.expire(),
            }
        }
        self.outcome.recv().unwrap_or(Err(CallError::Closed))
    }

    /// The outcome if it has arrived, or the call back to try again later.
    /// Never blocks, so a handler can poll a request it sent earlier.
    ///
    /// # Errors
    ///
    /// `Err(self)` while the request is still outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ());
    /// let call = endpoint.peer().request("x", ())?;
    /// let call = call.try_wait().err().unwrap(); // not answered yet
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":null}"#)?;
    /// assert!(call.try_wait().is_ok_and(|outcome| outcome.is_ok()));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn try_wait(self) -> Result<Result<Raw<'static>, CallError>, Call> {
        if self.deadline.is_some_and(|d| d <= Instant::now()) {
            self.link.expire();
        }
        match self.outcome.try_recv() {
            Ok(outcome) => Ok(outcome),
            Err(TryRecvError::Disconnected) => Ok(Err(CallError::Closed)),
            Err(TryRecvError::Empty) => Err(self),
        }
    }

    /// Cancels the request: the peer is sent the cancel notification and the
    /// outcome becomes [`CallError::Cancelled`]. Returns `false` if the
    /// request had already ended.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{CallError, Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let call = peer.request("slow", ())?;
    /// assert!(call.cancel());
    /// let sent: Vec<String> = rx.try_iter().collect();
    /// assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn cancel(self) -> bool {
        self.link.cancel(self.id)
    }

    /// A handle that can cancel this request from elsewhere (another thread,
    /// a later handler) while this `Call` waits.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{CallError, Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let call = peer.request("slow", ())?;
    /// let canceller = call.canceller();
    /// std::thread::spawn(move || canceller.cancel());
    /// assert_eq!(call.wait(), Err(CallError::Cancelled));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn canceller(&self) -> Canceller {
        Canceller {
            id: self.id,
            link: Arc::clone(&self.link),
        }
    }
}

impl fmt::Debug for Call {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Call")
            .field("id", &self.id)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Cancels one of our requests from anywhere. `Clone`, `Send`, `Sync`.
///
/// # Examples
///
/// ```
/// let (tx, _rx) = std::sync::mpsc::channel::<String>();
/// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ()); // keep it: dropping an endpoint closes it
/// let peer = endpoint.peer();
/// let call = peer.request("slow", ())?;
/// let canceller = call.canceller();
/// assert!(canceller.cancel());
/// assert!(!canceller.cancel()); // already ended
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone)]
pub struct Canceller {
    id: u64,
    link: Arc<dyn Link>,
}

impl Canceller {
    /// Cancels the request, as [`Call::cancel`]. Returns `false` if it had
    /// already ended.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let canceller = peer.request("x", ())?.canceller();
    /// assert!(canceller.cancel());
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn cancel(&self) -> bool {
        self.link.cancel(self.id)
    }

    /// The id of the request it cancels.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ()); // keep it: dropping an endpoint closes it
    /// let peer = endpoint.peer();
    /// let call = peer.request("x", ())?;
    /// assert_eq!(call.canceller().id(), call.id());
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub const fn id(&self) -> u64 {
        self.id
    }
}

impl fmt::Debug for Canceller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Canceller").field("id", &self.id).finish()
    }
}

/// The right to answer one deferred request, exactly once.
///
/// A handler registered with [`Router::deferred`](crate::Router::deferred)
/// receives one and may answer at once, keep it in its state and answer from
/// a later message, or move it to another thread. Answering consumes it; the
/// answer is sent immediately, from whichever thread answers.
///
/// Exactly one response is guaranteed: if the `Responder` is dropped without
/// answering (a worker returned early, a panic unwound through it), the
/// request is answered with `INTERNAL_ERROR`. If the peer cancels the
/// request first, its [`CancelToken`] is set and the cancellation is answered
/// as [`Cancellation`](crate::Cancellation) says; a later answer returns
/// [`Error::AlreadyAnswered`] and is not sent.
///
/// # Examples
///
/// ```
/// use std::sync::mpsc;
/// use jsonx_lib::json;
/// use rpc_io::{Endpoint, Router};
///
/// let router = Router::new().deferred("slow", |_, _params, _cx, responder| {
///     std::thread::spawn(move || {
///         // ... long work, checking `responder.is_cancelled()` ...
///         responder.ok(json!("done")).ok();
///     });
/// });
/// let (tx, rx) = mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, router, ());
/// endpoint.handle(br#"{"jsonrpc":"2.0","id":7,"method":"slow"}"#)?;
/// let reply = rx.recv_timeout(std::time::Duration::from_secs(5)).ok();
/// assert_eq!(reply.as_deref(), Some(r#"{"jsonrpc":"2.0","id":7,"result":"done"}"#));
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Responder {
    id: Id<'static>,
    token: CancelToken,
    link: Arc<dyn Link>,
    answered: bool,
}

impl Responder {
    pub(crate) fn new(id: Id<'static>, token: CancelToken, link: Arc<dyn Link>) -> Self {
        Self {
            id,
            token,
            link,
            answered: false,
        }
    }

    /// The id of the request being answered.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     assert_eq!(responder.id().as_i64(), Some(3));
    ///     responder.ok(()).ok();
    /// });
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[inline]
    #[must_use]
    pub fn id(&self) -> &Id<'static> {
        &self.id
    }

    /// A clone of the request's cancellation flag.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     assert!(!responder.token().is_cancelled());
    ///     responder.ok(()).ok();
    /// });
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn token(&self) -> CancelToken {
        self.token.clone()
    }

    /// Whether the peer cancelled the request (or the connection closed).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     assert!(!responder.is_cancelled());
    ///     responder.ok(()).ok();
    /// });
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// A [`Peer`] on the same connection, to send progress or ask questions
    /// while answering.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     responder.peer().notify("progress", ()).ok();
    ///     responder.ok(()).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// assert_eq!(rx.try_iter().count(), 2);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn peer(&self) -> Peer {
        Peer::new(Arc::clone(&self.link))
    }

    /// Answers with `result`: a value, or an error.
    ///
    /// # Errors
    ///
    /// [`Error::AlreadyAnswered`] if the peer cancelled the request first;
    /// [`Error::Closed`] if the connection has ended; [`Error::Io`] if writing
    /// failed.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, ErrorObject, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     responder.respond(Err::<(), _>(ErrorObject::internal_error("no"))).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.contains(r#""error":{"code":-32603"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn respond<'a, J: Into<Json<'a>>>(
        mut self,
        result: Result<J, ErrorObject>,
    ) -> Result<(), Error> {
        self.answered = true;
        match result {
            Ok(value) => self.link.respond(&self.id, Ok(&value.into())),
            Err(error) => self.link.respond(&self.id, Err(&error)),
        }
    }

    /// Answers with a result value.
    ///
    /// # Errors
    ///
    /// As [`respond`](Responder::respond).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     responder.ok(42).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":3,"result":42}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn ok<'a>(self, value: impl Into<Json<'a>>) -> Result<(), Error> {
        self.respond(Ok(value.into()))
    }

    /// Answers with an error.
    ///
    /// # Errors
    ///
    /// As [`respond`](Responder::respond).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, ErrorObject, Router};
    ///
    /// let router = Router::new().deferred("m", |_, _, _, responder| {
    ///     responder.err(ErrorObject::invalid_params("missing `uri`")).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32602")));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn err(self, error: ErrorObject) -> Result<(), Error> {
        self.respond(Err::<(), _>(error))
    }
}

impl Drop for Responder {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        let message = if thread::panicking() {
            "the handler panicked before answering"
        } else {
            "the handler dropped the request without answering"
        };
        // Already answered (cancelled) or closed: there is no one left to
        // tell, which is the point of answering here.
        let _settled = self
            .link
            .respond(&self.id, Err(&ErrorObject::internal_error(message)));
    }
}

impl fmt::Debug for Responder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Responder")
            .field("id", &self.id)
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}
