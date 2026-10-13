//! [`Router`]: which handler answers which method, and what a handler gets:
//! [`Params`] and a [`Context`]. [`Event`]s report what the endpoint did on
//! its own.

use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use jsonx_lib::{Parser, Value};

use crate::{
    error::Error,
    error_object::ErrorObject,
    framing::FrameError,
    handles::{Call, Peer, Responder},
    id::Id,
    link::Link,
    raw::{Json, Raw},
};

pub(crate) type RequestFn<S> = Box<
    dyn FnMut(&mut S, Params<'_>, &mut Context<'_>) -> Result<Json<'static>, ErrorObject> + Send,
>;
pub(crate) type DeferredFn<S> =
    Box<dyn FnMut(&mut S, Params<'_>, &mut Context<'_>, Responder) + Send>;
/// A notification handler; `Err` when it could not read its params (a typed
/// handler), which is reported, not answered.
pub(crate) type NotificationFn<S> =
    Box<dyn FnMut(&mut S, Params<'_>, &mut Context<'_>) -> Result<(), ErrorObject> + Send>;
pub(crate) type EventFn<S> = Box<dyn FnMut(&mut S, &Event<'_>) + Send>;
pub(crate) type InterceptFn<S> =
    Box<dyn FnMut(&mut S, &str, Option<&Id<'_>>, &Params<'_>) -> Result<(), ErrorObject> + Send>;

/// How a request method is answered.
pub(crate) enum RequestHandler<S> {
    /// By the handler's return value.
    Now(RequestFn<S>),
    /// Through a [`Responder`], now or later.
    Deferred(DeferredFn<S>),
}

/// Which handler answers which method.
///
/// Handlers receive the endpoint's state `S` (`&mut`), the call's
/// [`Params`], and a [`Context`] for talking back. They run one at a time, in
/// the order messages arrive, on the endpoint's thread. A request handler's
/// `Ok` value (anything that converts into [`Json`]) becomes the result and
/// its `Err` the error; a [deferred](Router::deferred) handler answers through
/// a [`Responder`] instead.
///
/// A panic in any handler is contained: a request is answered with
/// `INTERNAL_ERROR` (the panic message goes to [`Event::Panicked`], never to
/// the peer), a notification gets nothing, and the session carries on. The
/// state may be left half-updated by the panicking handler; the endpoint does
/// not try to repair it. Containment needs `panic = "unwind"` (Rust's
/// default): under `panic = "abort"` a handler panic ends the process.
///
/// Requests for methods with no request handler are answered with
/// `METHOD_NOT_FOUND`. Notifications for methods with no notification handler
/// are ignored (reported as [`Event::UnknownNotification`]): requests and
/// notifications are registered separately, as LSP and MCP use them.
///
/// # Examples
///
/// ```
/// use jsonx_lib::json;
/// use rpc_io::{ErrorObject, Router};
///
/// #[derive(Default)]
/// struct Counter { hits: u64 }
///
/// let router = Router::new()
///     .request("add", |_: &mut Counter, params, _| {
///         let p = params.parse()?;
///         match (p[0].as_i64(), p[1].as_i64()) {
///             (Some(a), Some(b)) => Ok(json!(a + b)),
///             _ => Err(ErrorObject::invalid_params("expected two integers")),
///         }
///     })
///     .notification("hit", |state, _, _| state.hits += 1);
/// assert!(router.has_request("add"));
/// assert!(router.has_notification("hit"));
/// ```
pub struct Router<S> {
    pub(crate) requests: BTreeMap<Box<str>, RequestHandler<S>>,
    pub(crate) notifications: BTreeMap<Box<str>, NotificationFn<S>>,
    pub(crate) on_event: Option<EventFn<S>>,
    pub(crate) intercept: Option<InterceptFn<S>>,
}

impl<S> Router<S> {
    /// A router with no handlers.
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> = rpc_io::Router::new();
    /// assert!(!router.has_request("anything"));
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            requests: BTreeMap::new(),
            notifications: BTreeMap::new(),
            on_event: None,
            intercept: None,
        }
    }

    /// Answers requests for `method` with `handler`'s return value,
    /// replacing any request handler registered for it before.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("ping", |_, _, _| Ok("pong"));
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":"pong"}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn request<F, J>(mut self, method: &str, mut handler: F) -> Self
    where
        F: FnMut(&mut S, Params<'_>, &mut Context<'_>) -> Result<J, ErrorObject> + Send + 'static,
        J: Into<Json<'static>>,
    {
        let boxed: RequestFn<S> = Box::new(
            move |state: &mut S, params: Params<'_>, cx: &mut Context<'_>| {
                handler(state, params, cx).map(Into::into)
            },
        );
        let _replaced = self
            .requests
            .insert(method.into(), RequestHandler::Now(boxed));
        self
    }

    /// Answers requests for `method` through a [`Responder`] the handler
    /// receives: now, from a later message, or from another thread. The
    /// request stays in flight (and cancellable by the peer) until answered;
    /// at most [`Config::max_in_flight`](crate::Config) can be, and more are
    /// refused with `SERVER_OVERLOADED`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::Router;
    ///
    /// let router: Router<()> = Router::new().deferred("index", |_, params, _, responder| {
    ///     let params = params.into_owned(); // keep it beyond this call
    ///     std::thread::spawn(move || {
    ///         let _ = params;
    ///         responder.ok("indexed").ok();
    ///     });
    /// });
    /// assert!(router.has_request("index"));
    /// ```
    #[must_use]
    pub fn deferred<F>(mut self, method: &str, handler: F) -> Self
    where
        F: FnMut(&mut S, Params<'_>, &mut Context<'_>, Responder) + Send + 'static,
    {
        let _replaced = self
            .requests
            .insert(method.into(), RequestHandler::Deferred(Box::new(handler)));
        self
    }

    /// Handles notifications for `method`, replacing any notification handler
    /// registered for it before.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().notification("log", |seen: &mut Vec<String>, params, _| {
    ///     seen.push(params.raw().map(|r| r.as_str().to_owned()).unwrap_or_default());
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, Vec::new());
    /// endpoint.handle(br#"{"jsonrpc":"2.0","method":"log","params":["hi"]}"#)?;
    /// assert_eq!(endpoint.state(), &["[\"hi\"]"]);
    /// assert!(rx.try_recv().is_err()); // notifications are never answered
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn notification<F>(self, method: &str, mut handler: F) -> Self
    where
        F: FnMut(&mut S, Params<'_>, &mut Context<'_>) + Send + 'static,
    {
        self.notification_with(method, move |state, params, cx| {
            handler(state, params, cx);
            Ok(())
        })
    }

    /// Registers a notification handler that may refuse its params.
    pub(crate) fn notification_with<F>(mut self, method: &str, handler: F) -> Self
    where
        F: FnMut(&mut S, Params<'_>, &mut Context<'_>) -> Result<(), ErrorObject> + Send + 'static,
    {
        let _replaced = self.notifications.insert(method.into(), Box::new(handler));
        self
    }

    /// Calls `hook` for every [`Event`]: things the endpoint handled on its
    /// own that an application may want to log. Runs on the endpoint's thread;
    /// a panic in it is contained and ignored.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Event, Router};
    ///
    /// let router = Router::new().on_event(|log: &mut Vec<String>, event| {
    ///     if let Event::UnknownNotification { method } = event {
    ///         log.push(method.to_string());
    ///     }
    /// });
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, Vec::new());
    /// endpoint.handle(br#"{"jsonrpc":"2.0","method":"nobody/listens"}"#)?;
    /// assert_eq!(endpoint.state(), &["nobody/listens"]);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn on_event<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&mut S, &Event<'_>) + Send + 'static,
    {
        self.on_event = Some(Box::new(hook));
        self
    }

    /// Runs `hook` before every call the router would route: each request
    /// and each notification, after the gate and cancellation have had their
    /// say and before the handler is looked up. `Ok(())` lets the call
    /// through; `Err(error)` refuses it: a request is answered with `error`,
    /// a notification is dropped and reported as [`Event::Refused`]. The
    /// place for authorization, rate limits, or an audit log. The hook gets
    /// the state, the method, the id (`None` for a notification), and the
    /// params; a panic in it is contained as a handler's would be.
    /// Registering a hook again replaces it.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, ErrorCode, ErrorObject, Router};
    ///
    /// #[derive(Default)]
    /// struct Session { calls: u32, admin: bool }
    ///
    /// let router = Router::new()
    ///     .request("admin/reset", |_: &mut Session, _, _| Ok(()))
    ///     .request("ping", |_, _, _| Ok("pong"))
    ///     .intercept(|session, method, _id, _params| {
    ///         session.calls += 1;
    ///         if method.starts_with("admin/") && !session.admin {
    ///             return Err(ErrorObject::new(ErrorCode(1003), "forbidden"));
    ///         }
    ///         Ok(())
    ///     });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, Session::default());
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"admin/reset"}"#)?;
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#)?;
    /// let replies: Vec<String> = rx.try_iter().collect();
    /// assert_eq!(replies[0], r#"{"jsonrpc":"2.0","id":1,"error":{"code":1003,"message":"forbidden"}}"#);
    /// assert_eq!(replies[1], r#"{"jsonrpc":"2.0","id":2,"result":"pong"}"#);
    /// assert_eq!(endpoint.state().calls, 2);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn intercept<F>(mut self, hook: F) -> Self
    where
        F: FnMut(&mut S, &str, Option<&Id<'_>>, &Params<'_>) -> Result<(), ErrorObject>
            + Send
            + 'static,
    {
        self.intercept = Some(Box::new(hook));
        self
    }

    /// Whether a request handler is registered for `method`.
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> = rpc_io::Router::new().request("a", |_, _, _| Ok(()));
    /// assert!(router.has_request("a"));
    /// assert!(!router.has_notification("a"));
    /// ```
    #[must_use]
    pub fn has_request(&self, method: &str) -> bool {
        self.requests.contains_key(method)
    }

    /// Whether a notification handler is registered for `method`.
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> = rpc_io::Router::new().notification("n", |_, _, _| {});
    /// assert!(router.has_notification("n"));
    /// ```
    #[must_use]
    pub fn has_notification(&self, method: &str) -> bool {
        self.notifications.contains_key(method)
    }
}

impl<S> Default for Router<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> fmt::Debug for Router<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Router")
            .field("requests", &self.requests.keys().collect::<Vec<_>>())
            .field(
                "notifications",
                &self.notifications.keys().collect::<Vec<_>>(),
            )
            .finish_non_exhaustive()
    }
}

/// A call's `params`: validated, kept as a slice of the message, parsed only
/// when asked.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Router};
///
/// let router = Router::new().request("pos", |_, params, _| {
///     let p = params.parse()?; // `INVALID_PARAMS` if over the parser's budgets
///     let line = p["line"].as_u64().ok_or_else(|| {
///         rpc_io::ErrorObject::invalid_params("`line` must be a non-negative integer")
///     })?;
///     Ok(line + 1)
/// });
/// let (tx, rx) = std::sync::mpsc::channel::<String>();
/// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"pos","params":{"line":9}}"#)?;
/// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":10}"#));
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone, Debug)]
pub struct Params<'a> {
    raw: Option<Raw<'a>>,
    parser: Parser,
}

impl<'a> Params<'a> {
    pub(crate) fn new(raw: Option<Raw<'a>>, parser: Parser) -> Self {
        Self { raw, parser }
    }

    /// The parser with the endpoint's budgets.
    pub(crate) fn parser(&self) -> Parser {
        self.parser
    }

    /// The params' JSON text, `None` when absent.
    pub(crate) fn text(&self) -> Option<&str> {
        self.raw.as_ref().map(Raw::as_str)
    }

    /// Parses the params (an array or an object; `null` when absent), with
    /// the endpoint's JSON budgets and duplicate keys refused. The value
    /// borrows from the message.
    ///
    /// # Errors
    ///
    /// An `INVALID_PARAMS` [`ErrorObject`] saying why, ready to return with
    /// `?`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("dup", |_, params, _| params.parse().map(|_| ()));
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"dup","params":{"a":1,"a":2}}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32602")));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn parse(&self) -> Result<Value<'_>, ErrorObject> {
        match &self.raw {
            Some(raw) => raw.parse_with(&self.parser).map_err(ErrorObject::from),
            None => Ok(Value::Null),
        }
    }

    /// The params as raw JSON text, `None` when absent.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("echo", |_, params, _| {
    ///     Ok(params.raw().map(|r| r.clone().into_owned()).unwrap_or(rpc_io::Raw::NULL))
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"echo","params":[1, 2]}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":[1, 2]}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn raw(&self) -> Option<&Raw<'a>> {
        self.raw.as_ref()
    }

    /// Whether params were sent.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("m", |_, params, _| Ok(params.is_present()));
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"m"}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.ends_with(r#""result":false}"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn is_present(&self) -> bool {
        self.raw.is_some()
    }

    /// Copies the params so they can outlive the message, to move them to
    /// another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Params, Router};
    ///
    /// let router = Router::<Vec<Params<'static>>>::new().notification("keep", |kept, params, _| {
    ///     kept.push(params.into_owned());
    /// });
    /// # let _ = router;
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Params<'static> {
        Params {
            raw: self.raw.map(Raw::into_owned),
            parser: self.parser,
        }
    }

    /// The params as raw JSON, `None` when absent.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// // Forward a request's params to the peer unchanged: no parse, no re-serialization.
    /// let router = Router::new().notification("relay", |_, params, cx| {
    ///     let raw = params.into_raw().unwrap_or(rpc_io::Raw::NULL);
    ///     cx.notify("relayed", raw).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","method":"relay","params":{"k": [1]}}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","method":"relayed","params":{"k": [1]}}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn into_raw(self) -> Option<Raw<'a>> {
        self.raw
    }
}

/// What a handler can reach while it runs: the method and id it is handling,
/// and a channel back to the peer.
///
/// Messages sent through a context during a request go out before that
/// request's response, in the order they were sent.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Router};
///
/// let router = Router::new().request("work", |_, _, cx| {
///     cx.notify("progress", jsonx_lib::json!({ "method": cx.method() }))?;
///     Ok(())
/// });
/// let (tx, rx) = std::sync::mpsc::channel::<String>();
/// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#)?;
/// let sent: Vec<String> = rx.try_iter().collect();
/// assert_eq!(sent[0], r#"{"jsonrpc":"2.0","method":"progress","params":{"method":"work"}}"#);
/// assert_eq!(sent[1], r#"{"jsonrpc":"2.0","id":1,"result":null}"#);
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Context<'a> {
    link: &'a Arc<dyn Link>,
    stop: &'a mut bool,
    id: Option<&'a Id<'a>>,
    method: &'a str,
}

impl<'a> Context<'a> {
    pub(crate) fn new(
        link: &'a Arc<dyn Link>,
        stop: &'a mut bool,
        id: Option<&'a Id<'a>>,
        method: &'a str,
    ) -> Self {
        Self {
            link,
            stop,
            id,
            method,
        }
    }

    /// The method being handled.
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> = rpc_io::Router::new().request("m", |_, _, cx| Ok(cx.method().len()));
    /// # let _ = router;
    /// ```
    #[must_use]
    pub fn method(&self) -> &str {
        self.method
    }

    /// The id of the request being handled; `None` for a notification.
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> =
    ///     rpc_io::Router::new().request("m", |_, _, cx| Ok(cx.id().and_then(|id| id.as_i64())));
    /// # let _ = router;
    /// ```
    #[must_use]
    pub fn id(&self) -> Option<&Id<'a>> {
        self.id
    }

    /// Sends a notification to the peer; see [`Peer::notify`].
    ///
    /// # Errors
    ///
    /// As [`Peer::notify`].
    ///
    /// # Examples
    ///
    /// ```
    /// let router: rpc_io::Router<()> = rpc_io::Router::new().notification("n", |_, _, cx| {
    ///     cx.notify("ack", ()).ok();
    /// });
    /// # let _ = router;
    /// ```
    pub fn notify<'j>(&self, method: &str, params: impl Into<Json<'j>>) -> Result<(), Error> {
        Peer::new(Arc::clone(self.link)).notify(method, params)
    }

    /// Sends a request to the peer; see [`Peer::request`]. Its [`Call`]
    /// cannot be waited on here (the answer arrives on this thread, after the
    /// handler returns): keep it and [`try_wait`](Call::try_wait) later, or
    /// wait on another thread.
    ///
    /// # Errors
    ///
    /// As [`Peer::request`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Call, Router};
    ///
    /// let router = Router::<Vec<Call>>::new().notification("initialized", |calls, _, cx| {
    ///     if let Ok(call) = cx.request("workspace/configuration", jsonx_lib::json!({ "items": [] })) {
    ///         calls.push(call);
    ///     }
    /// });
    /// # let _ = router;
    /// ```
    pub fn request<'j>(&self, method: &str, params: impl Into<Json<'j>>) -> Result<Call, Error> {
        Peer::new(Arc::clone(self.link)).request(method, params)
    }

    /// Sends a request with a timeout; see [`Peer::request_with_timeout`].
    ///
    /// # Errors
    ///
    /// As [`Peer::request`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let router: rpc_io::Router<()> = rpc_io::Router::new().notification("n", |_, _, cx| {
    ///     let _call = cx.request_with_timeout("ask", (), Duration::from_secs(1));
    /// });
    /// # let _ = router;
    /// ```
    pub fn request_with_timeout<'j>(
        &self,
        method: &str,
        params: impl Into<Json<'j>>,
        timeout: Duration,
    ) -> Result<Call, Error> {
        Peer::new(Arc::clone(self.link)).request_with_timeout(method, params, timeout)
    }

    /// A [`Peer`] for this connection, to keep or move to another thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Peer, Router};
    ///
    /// let router: Router<Option<Peer>> = Router::new().notification("hello", |slot, _, cx| {
    ///     *slot = Some(cx.peer());
    /// });
    /// # let _ = router;
    /// ```
    #[must_use]
    pub fn peer(&self) -> Peer {
        Peer::new(Arc::clone(self.link))
    }

    /// Stops the endpoint after this message: [`Endpoint::run`](crate::Endpoint::run)
    /// returns [`End::Stopped`](crate::End) and
    /// [`Endpoint::handle`](crate::Endpoint::handle) returns
    /// [`Flow::Stop`](crate::Flow). A response to the current request is still
    /// sent.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Flow, Router};
    ///
    /// let router = Router::new().notification("exit", |_, _, cx| cx.stop());
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, ());
    /// assert_eq!(endpoint.handle(br#"{"jsonrpc":"2.0","method":"exit"}"#)?, Flow::Stop);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn stop(&mut self) {
        *self.stop = true;
    }
}

impl fmt::Debug for Context<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Context")
            .field("method", &self.method)
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

/// Something the endpoint handled on its own, reported to
/// [`Router::on_event`] so an application can log it.
///
/// # Examples
///
/// ```
/// use rpc_io::Event;
///
/// fn describe(event: &Event<'_>) -> String {
///     match event {
///         Event::Panicked { method, message } => format!("`{method}` panicked: {message}"),
///         other => format!("{other:?}"),
///     }
/// }
/// assert_eq!(describe(&Event::Panicked { method: "m", message: "boom" }), "`m` panicked: boom");
/// ```
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum Event<'a> {
    /// A handler panicked. A request was answered with `INTERNAL_ERROR`
    /// (without the panic message, which may hold details the peer should not
    /// see); the message is here.
    Panicked {
        /// The method whose handler panicked.
        method: &'a str,
        /// The panic message, clipped, or a placeholder.
        message: &'a str,
    },
    /// A malformed message arrived: answered with `error` if it was
    /// request-like, dropped if it was a malformed response to one of our
    /// requests (which then ends with `CallError::InvalidResponse`).
    Invalid {
        /// What was wrong.
        error: &'a ErrorObject,
        /// Whether it was answered.
        answered: bool,
    },
    /// A notification for a method with no notification handler.
    UnknownNotification {
        /// The method.
        method: &'a str,
    },
    /// A notification dropped because it arrived before the
    /// [`Gate`](crate::Gate) opened.
    Dropped {
        /// The method.
        method: &'a str,
    },
    /// A response whose id matches none of our outstanding requests (late,
    /// repeated, or forged).
    Unmatched {
        /// The id it carried.
        id: &'a Id<'a>,
    },
    /// An error response with a `null` id: the peer could not read something
    /// we sent.
    PeerError {
        /// The peer's error.
        error: &'a ErrorObject,
    },
    /// A message over [`Limits::max_message_bytes`](crate::Limits) was skipped
    /// unread, and answered with `INVALID_REQUEST` under a `null` id.
    Oversized {
        /// The framing error with the sizes.
        error: FrameError,
    },
    /// The peer cancelled one of its requests that was in flight.
    Cancelled {
        /// The cancelled request's id.
        id: &'a Id<'a>,
    },
    /// A notification refused by the router's
    /// [interceptor](Router::intercept); it was not handled.
    Refused {
        /// The method.
        method: &'a str,
        /// The interceptor's error.
        error: &'a ErrorObject,
    },
    /// A notification's params did not fit what its handler takes (a
    /// [typed](Router::notification_typed) handler, with the `serde`
    /// feature); the handler was not called.
    InvalidParams {
        /// The method.
        method: &'a str,
        /// What did not fit, as an `INVALID_PARAMS` error.
        error: &'a ErrorObject,
    },
}
