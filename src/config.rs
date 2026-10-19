//! [`Config`]: how an endpoint behaves, with [`Gate`] and [`Cancellation`].

use std::{borrow::Cow, time::Duration};

use crate::{error_object::ErrorCode, limits::Limits};

/// An endpoint's settings.
///
/// | Setting | Default | Meaning |
/// |---|---|---|
/// | `limits` | [`Limits::new`] | budgets on incoming messages |
/// | `max_pending` | 1024 | our requests awaiting answers; one more evicts the oldest |
/// | `max_in_flight` | 1024 | deferred requests being answered; more are refused with `overloaded_code` |
/// | `max_held_bytes` | 4 MiB | messages this side sends while the gate is shut, held until it opens |
/// | `request_timeout` | none | deadline for our requests that set none themselves |
/// | `cancellation` | [`Cancellation::lsp`] | the cancel notification, both ways; `None` turns cancellation off |
/// | `gate` | none | an opening request every other request must wait for |
/// | `timer` | `true` | deadlines fire on their own, from a helper thread |
/// | `omit_null_id` | `false` | an error for a message whose id could not be read carries `"id": null` (JSON-RPC 2.0); `true` leaves `id` out (MCP) |
/// | `overloaded_code` | `SERVER_OVERLOADED` (`-32000`) | the code of the refusal past `max_in_flight` |
///
/// `#[non_exhaustive]`: start from [`Config::new`] and chain `with_*`.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use rpc_io::{Cancellation, Config, Gate};
///
/// let lsp = Config::new()
///     .with_gate(Some(Gate::new("initialize").allow("exit")))
///     .with_request_timeout(Some(Duration::from_secs(30)));
/// let mcp = Config::new().with_cancellation(Some(Cancellation::mcp()));
/// assert_eq!(lsp.max_pending, 1024);
/// assert_eq!(mcp.cancellation.map(|c| c.id_member), Some("requestId".into()));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Budgets on incoming messages.
    pub limits: Limits,
    /// Most of our requests awaiting an answer at once (at least one).
    pub max_pending: usize,
    /// Most deferred requests being answered at once.
    pub max_in_flight: usize,
    /// Most bytes of messages held back while the [`Gate`] is shut. A send
    /// past it fails with [`Error::Backlog`](crate::Error::Backlog) and is
    /// not held.
    pub max_held_bytes: usize,
    /// The deadline for our requests that do not set their own.
    pub request_timeout: Option<Duration>,
    /// How cancellation is spelled, or `None` for none.
    pub cancellation: Option<Cancellation>,
    /// The opening request, if the protocol has one.
    pub gate: Option<Gate>,
    /// Whether our requests' deadlines fire on their own. On (the default),
    /// the first request with a deadline starts a timer thread for the
    /// connection, which ends each due request with
    /// [`CallError::TimedOut`](crate::CallError::TimedOut) and sends the
    /// peer the cancel notification even while nothing arrives; it sleeps
    /// between deadlines and stops when the connection closes. Off, a
    /// deadline is enforced only when it is next looked at: when the endpoint
    /// handles a message, at the next request, in
    /// [`Call::wait`](crate::Call::wait) and
    /// [`try_wait`](crate::Call::try_wait), when an awaited `Call` is polled,
    /// or by [`Endpoint::expire`](crate::Endpoint::expire).
    pub timer: bool,
    /// Whether an error answering a message whose id could not be read
    /// leaves `id` out (`true`, as MCP's schema has it: `id?: RequestId`,
    /// never `null`) or writes `"id": null` (`false`, as JSON-RPC 2.0
    /// requires). Either form from the peer is read as the same answer.
    pub omit_null_id: bool,
    /// The error code a request past `max_in_flight` is refused with.
    pub overloaded_code: ErrorCode,
}

impl Config {
    /// The defaults in the table above.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::Config::new().gate.is_none());
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Self {
            limits: Limits::new(),
            max_pending: 1024,
            max_in_flight: 1024,
            max_held_bytes: 4 * 1024 * 1024,
            request_timeout: None,
            cancellation: Some(Cancellation::lsp()),
            gate: None,
            timer: true,
            omit_null_id: false,
            overloaded_code: ErrorCode::SERVER_OVERLOADED,
        }
    }

    /// Sets `limits`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, Limits};
    ///
    /// let c = Config::new().with_limits(Limits::new().with_max_batch(4));
    /// assert_eq!(c.limits.max_batch, 4);
    /// ```
    #[must_use]
    pub fn with_limits(mut self, limits: Limits) -> Self {
        self.limits = limits;
        self
    }

    /// Sets `max_pending`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Config::new().with_max_pending(8).max_pending, 8);
    /// ```
    #[must_use]
    pub fn with_max_pending(mut self, count: usize) -> Self {
        self.max_pending = count;
        self
    }

    /// Sets `max_in_flight`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Config::new().with_max_in_flight(8).max_in_flight, 8);
    /// ```
    #[must_use]
    pub fn with_max_in_flight(mut self, count: usize) -> Self {
        self.max_in_flight = count;
        self
    }

    /// Sets `max_held_bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Config::new().with_max_held_bytes(1 << 10).max_held_bytes, 1 << 10);
    /// ```
    #[must_use]
    pub fn with_max_held_bytes(mut self, bytes: usize) -> Self {
        self.max_held_bytes = bytes;
        self
    }

    /// Sets `request_timeout`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let c = rpc_io::Config::new().with_request_timeout(Some(Duration::from_secs(5)));
    /// assert_eq!(c.request_timeout, Some(Duration::from_secs(5)));
    /// ```
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Sets `cancellation`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::Config::new().with_cancellation(None).cancellation.is_none());
    /// ```
    #[must_use]
    pub fn with_cancellation(mut self, cancellation: Option<Cancellation>) -> Self {
        self.cancellation = cancellation;
        self
    }

    /// Sets `gate`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, Gate};
    ///
    /// let c = Config::new().with_gate(Some(Gate::new("initialize")));
    /// assert_eq!(c.gate.map(|g| g.method), Some("initialize".into()));
    /// ```
    #[must_use]
    pub fn with_gate(mut self, gate: Option<Gate>) -> Self {
        self.gate = gate;
        self
    }

    /// Sets `timer`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(!rpc_io::Config::new().with_timer(false).timer);
    /// ```
    #[must_use]
    pub fn with_timer(mut self, timer: bool) -> Self {
        self.timer = timer;
        self
    }

    /// Sets `omit_null_id`: whether an error for a message whose id could
    /// not be read leaves `id` out instead of writing `"id": null`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let config = Config::new().with_omit_null_id(true);
    /// Endpoint::new(tx, Router::<()>::new(), ()).with_config(config).handle(b"{")?;
    /// let reply = rx.try_recv().unwrap_or_default();
    /// assert!(reply.starts_with(r#"{"jsonrpc":"2.0","error":{"code":-32700"#), "{reply}");
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn with_omit_null_id(mut self, omit: bool) -> Self {
        self.omit_null_id = omit;
        self
    }

    /// Sets `overloaded_code`, the code of the refusal past
    /// `max_in_flight`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, ErrorCode};
    ///
    /// let config = Config::new().with_overloaded_code(ErrorCode::INTERNAL_ERROR);
    /// assert_eq!(config.overloaded_code, ErrorCode::INTERNAL_ERROR);
    /// ```
    #[must_use]
    pub fn with_overloaded_code(mut self, code: ErrorCode) -> Self {
        self.overloaded_code = code;
        self
    }
}

impl Config {
    /// An LSP server: [`Limits::lsp`], [`Cancellation::lsp`], and
    /// [`Gate::lsp`]. A client leaves the gate out (`with_gate(None)`): the
    /// gate belongs on the side that receives `initialize`.
    ///
    /// # Examples
    ///
    /// ```
    /// let config = rpc_io::Config::lsp();
    /// assert_eq!(config.gate.map(|g| g.method), Some("initialize".into()));
    /// assert_eq!(config.cancellation.map(|c| c.method), Some("$/cancelRequest".into()));
    /// ```
    #[must_use]
    pub fn lsp() -> Self {
        Self::new()
            .with_limits(Limits::lsp())
            .with_cancellation(Some(Cancellation::lsp()))
            .with_gate(Some(Gate::lsp()))
    }

    /// MCP as its current revision (2026-07-28) has it, for a server or a
    /// client: [`Limits::mcp`] (batches off), [`Cancellation::mcp`], and no
    /// gate. The protocol is stateless: every request carries its protocol
    /// version in `_meta`, and there is no `initialize` to wait for. Errors
    /// for unreadable messages are written without `id` (MCP's schema has
    /// no `null` id), and requests past `max_in_flight` are refused with
    /// `INTERNAL_ERROR` (`-32603`), since MCP asks new implementations not to
    /// use `-32000..=-32019`.
    ///
    /// This is also the preset of a dual-era server (one that serves legacy
    /// clients too): the specification has such a server pick each
    /// request's semantics from the request itself (an `initialize` selects
    /// the legacy ones), which belongs to the protocol layer above. A server
    /// for legacy clients only takes [`Config::mcp_legacy`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, ErrorCode};
    ///
    /// let config = Config::mcp();
    /// assert_eq!(config.limits.max_batch, 0);
    /// assert!(config.gate.is_none());
    /// assert!(config.omit_null_id);
    /// assert_eq!(config.overloaded_code, ErrorCode::INTERNAL_ERROR);
    /// assert_eq!(config.cancellation.map(|c| c.id_member), Some("requestId".into()));
    /// ```
    #[must_use]
    pub fn mcp() -> Self {
        Self::new()
            .with_limits(Limits::mcp())
            .with_cancellation(Some(Cancellation::mcp()))
            .with_omit_null_id(true)
            .with_overloaded_code(ErrorCode::INTERNAL_ERROR)
    }

    /// A server for MCP's legacy revisions only (2025-11-25 and earlier),
    /// which open with an `initialize` handshake: [`Config::mcp`] plus
    /// [`Gate::mcp`]. Modern clients never send `initialize`, so this
    /// preset refuses every request of theirs: use it only where no modern
    /// client can connect. A client leaves the gate out (`with_gate(None)`).
    ///
    /// # Examples
    ///
    /// ```
    /// let config = rpc_io::Config::mcp_legacy();
    /// assert_eq!(config.gate.map(|g| g.method), Some("initialize".into()));
    /// assert!(config.omit_null_id);
    /// ```
    #[must_use]
    pub fn mcp_legacy() -> Self {
        Self::mcp().with_gate(Some(Gate::mcp()))
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new()
    }
}

/// How a protocol spells request cancellation: a notification naming the id
/// of the request to cancel.
///
/// Both directions use it. When the peer sends it for one of our deferred
/// requests, the request's [`CancelToken`](crate::CancelToken) is set and, if
/// `reply` is set, the request is answered at once with that error code (LSP:
/// a cancelled request still gets exactly one response); with `reply: None`
/// it gets no response at all (MCP: the receiver "SHOULD NOT send a response
/// for the cancelled request"), and a reply the handler sends later is
/// dropped. When one of our requests times out, is evicted, or is cancelled,
/// the peer is sent this notification.
///
/// The cancel notification is consumed by the endpoint; it never reaches the
/// [`Router`](crate::Router).
///
/// # Examples
///
/// ```
/// use rpc_io::{Cancellation, ErrorCode};
///
/// let lsp = Cancellation::lsp();
/// assert_eq!((lsp.method.as_ref(), lsp.id_member.as_ref()), ("$/cancelRequest", "id"));
/// assert_eq!(lsp.reply, Some(ErrorCode::REQUEST_CANCELLED));
///
/// // A service API of your own: `$/cancel {id}`, answered with error 1005.
/// let custom = Cancellation::new("$/cancel", "id", Some(ErrorCode(1005)));
/// assert_eq!(custom.reply, Some(ErrorCode(1005)));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Cancellation {
    /// The notification's method.
    pub method: Cow<'static, str>,
    /// The `params` member holding the request id.
    pub id_member: Cow<'static, str>,
    /// The error code a cancelled request is answered with, or `None` to send
    /// no response.
    pub reply: Option<ErrorCode>,
}

impl Cancellation {
    /// A cancellation spelled `method {id_member: <id>}`.
    ///
    /// # Examples
    ///
    /// ```
    /// let c = rpc_io::Cancellation::new("cancel", "target", None);
    /// assert_eq!(c.id_member, "target");
    /// ```
    #[must_use]
    pub fn new(
        method: impl Into<Cow<'static, str>>,
        id_member: impl Into<Cow<'static, str>>,
        reply: Option<ErrorCode>,
    ) -> Self {
        Self {
            method: method.into(),
            id_member: id_member.into(),
            reply,
        }
    }

    /// LSP: `$/cancelRequest {id}`, answered with `RequestCancelled`
    /// (`-32800`).
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Cancellation::lsp().method, "$/cancelRequest");
    /// ```
    #[must_use]
    pub fn lsp() -> Self {
        Self::new("$/cancelRequest", "id", Some(ErrorCode::REQUEST_CANCELLED))
    }

    /// MCP: `notifications/cancelled {requestId}`, and no response for the
    /// cancelled request.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Cancellation::mcp().reply, None);
    /// ```
    #[must_use]
    pub fn mcp() -> Self {
        Self::new("notifications/cancelled", "requestId", None)
    }
}

/// An opening request that must succeed before anything else is served (LSP's
/// `initialize`, MCP's `initialize`, a service's own `auth`).
///
/// Until the opening request has been answered successfully:
///
/// - other requests are answered with `code` (default
///   [`SERVER_NOT_INITIALIZED`](ErrorCode::SERVER_NOT_INITIALIZED)), except
///   the methods in `allow_requests` (MCP's `ping`);
/// - notifications are dropped (reported as [`Event::Dropped`](crate::Event)),
///   except the methods in `allow` (LSP's `exit`);
/// - everything this side sends other than responses (notifications and
///   requests, from handlers or other threads) is held back, then sent in
///   order right after the opening response; if the opening request fails,
///   what was held is discarded and held requests end with
///   [`CallError::Cancelled`](crate::CallError::Cancelled). A held request
///   that times out or is cancelled is simply never sent, and an "answer" to
///   one is treated as unmatched: the peer has never seen it.
///
/// The opening request fails when it is answered with an error, and also when
/// the peer cancels it under a [`Cancellation`] that sends no reply; the gate
/// then shuts again and a new opening request is served. Afterwards (once it
/// succeeded) a repeated opening request is answered with `INVALID_REQUEST`,
/// as is an opening request inside a batch. A gate belongs on the side that
/// receives the opening request; on the side that sends it, it would hold that
/// side's own messages forever.
///
/// Held messages are bounded by [`Config::max_held_bytes`]: a send that would
/// take them past it fails with [`Error::Backlog`](crate::Error::Backlog)
/// (and a request that fails so is not outstanding). Held requests also count
/// against [`Config::max_pending`].
///
/// # Examples
///
/// ```
/// use rpc_io::{ErrorCode, Gate};
///
/// let gate = Gate::new("initialize").allow("exit");
/// assert_eq!(gate.code, ErrorCode::SERVER_NOT_INITIALIZED);
/// assert_eq!(gate.allow, ["exit"]);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Gate {
    /// The opening request's method.
    pub method: Cow<'static, str>,
    /// The error code for requests that arrive before the gate opens.
    pub code: ErrorCode,
    /// Notifications delivered even before the gate opens.
    pub allow: Vec<Cow<'static, str>>,
    /// Requests served even before the gate opens (MCP's `ping`).
    pub allow_requests: Vec<Cow<'static, str>>,
}

impl Gate {
    /// A gate opened by a successful `method` request.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::Gate::new("auth").allow.is_empty());
    /// ```
    #[must_use]
    pub fn new(method: impl Into<Cow<'static, str>>) -> Self {
        Self {
            method: method.into(),
            code: ErrorCode::SERVER_NOT_INITIALIZED,
            allow: Vec::new(),
            allow_requests: Vec::new(),
        }
    }

    /// LSP's: `initialize`, with `exit` let through (a client may end a
    /// server it never initialized).
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Gate::lsp().allow, ["exit"]);
    /// ```
    #[must_use]
    pub fn lsp() -> Self {
        Self::new("initialize").allow("exit")
    }

    /// MCP's legacy handshake (revisions 2025-11-25 and earlier; the
    /// 2026-07-28 revision has none): `initialize`, with the `ping` request
    /// served before it (those revisions allow pings at any time), and
    /// early requests refused with `INVALID_REQUEST` (`-32600`). Not
    /// `SERVER_NOT_INITIALIZED`: MCP used `-32002` for "resource not found"
    /// in those revisions and forbids emitting it since.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, Gate};
    ///
    /// assert_eq!(Gate::mcp().allow_requests, ["ping"]);
    /// assert_eq!(Gate::mcp().code, ErrorCode::INVALID_REQUEST);
    /// ```
    #[must_use]
    pub fn mcp() -> Self {
        Self::new("initialize")
            .allow_request("ping")
            .with_code(ErrorCode::INVALID_REQUEST)
    }

    /// Serves the request `method` before the gate opens.
    ///
    /// # Examples
    ///
    /// ```
    /// let gate = rpc_io::Gate::new("auth").allow_request("version");
    /// assert_eq!(gate.allow_requests, ["version"]);
    /// ```
    #[must_use]
    pub fn allow_request(mut self, method: impl Into<Cow<'static, str>>) -> Self {
        self.allow_requests.push(method.into());
        self
    }

    /// Whether the request `method` is served before the gate opens.
    pub(crate) fn allows_request(&self, method: &str) -> bool {
        self.allow_requests.iter().any(|m| m == method)
    }

    /// Lets the notification `method` through before the gate opens.
    ///
    /// # Examples
    ///
    /// ```
    /// let gate = rpc_io::Gate::new("initialize").allow("exit").allow("$/logTrace");
    /// assert_eq!(gate.allow.len(), 2);
    /// ```
    #[must_use]
    pub fn allow(mut self, method: impl Into<Cow<'static, str>>) -> Self {
        self.allow.push(method.into());
        self
    }

    /// Sets the error code for requests before the gate opens.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, Gate};
    ///
    /// assert_eq!(Gate::new("auth").with_code(ErrorCode(1010)).code, ErrorCode(1010));
    /// ```
    #[must_use]
    pub fn with_code(mut self, code: ErrorCode) -> Self {
        self.code = code;
        self
    }

    /// Whether the notification `method` passes before the gate opens.
    pub(crate) fn allows(&self, method: &str) -> bool {
        self.allow.iter().any(|m| m == method)
    }
}
