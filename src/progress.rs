//! Progress notifications: MCP's `notifications/progress` ([`Progress`]) and
//! LSP's `$/progress` work-done reports ([`WorkDone`]), each tied to the
//! token the peer put in its request.

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

use crate::{
    convert::with_buffer,
    error::Error,
    handles::{Peer, Responder},
    link::Scope,
    message::{is_token, read_member},
    raw::{Json, Raw},
    router::{Context, Params},
};

impl Params<'_> {
    /// The MCP progress token of the request: `params._meta.progressToken`,
    /// a string or an integer. `None` when the peer asked for no progress.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("tools/call", |_: &mut (), params, _| {
    ///     Ok(params.progress_token().map(|t| t.as_str().to_owned()))
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(
    ///     br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x","_meta":{"progressToken":"p7"}}}"#,
    /// )?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":"\"p7\""}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn progress_token(&self) -> Option<Raw<'_>> {
        let parser = self.parser();
        let meta = read_member(self.text()?, "_meta", &parser)?;
        token(read_member(meta, "progressToken", &parser)?)
    }

    /// The LSP work-done token of the request: `params.workDoneToken`, a
    /// string or an integer. `None` when the client asked for no progress.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().request("workspace/symbol", |_: &mut (), params, _| {
    ///     Ok(params.work_done_token().is_some())
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(
    ///     br#"{"jsonrpc":"2.0","id":1,"method":"workspace/symbol","params":{"query":"","workDoneToken":4}}"#,
    /// )?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.ends_with(r#""result":true}"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn work_done_token(&self) -> Option<Raw<'_>> {
        token(read_member(self.text()?, "workDoneToken", &self.parser())?)
    }
}

/// `raw` as a token, if it is a string or an integer.
fn token(raw: &str) -> Option<Raw<'_>> {
    is_token(raw).then(|| Raw::trusted(raw))
}

/// `Ok` for a finite `n`: JSON has no NaN or infinity.
fn finite(name: &str, n: f64) -> Result<(), Error> {
    if n.is_finite() {
        Ok(())
    } else {
        Err(Error::Serialize(format!(
            "`{name}` must be a finite number, not {n}"
        )))
    }
}

/// Appends `"name":<n>` for a finite `n`: whole numbers without a fraction
/// (`3`, not `3.0`), as peers written in JavaScript send them, and any other
/// as its shortest round-trip text.
fn push_number(out: &mut String, name: &str, n: f64) {
    /// 2^53: every whole `f64` below it is exactly an `i64`.
    const EXACT: f64 = 9_007_199_254_740_992.0;
    out.push(',');
    jsonx_lib::write_string(out, name);
    out.push(':');
    if n.fract() == 0.0 && n.abs() < EXACT {
        // Whole and below 2^53 in size: the cast is exact.
        crate::text::push_i64(out, n as i64);
    } else {
        jsonx_lib::write_value(out, &n);
    }
}

/// Appends `"name":"<text>"` when there is a text.
fn push_text(out: &mut String, name: &str, text: Option<&str>) {
    if let Some(text) = text {
        out.push(',');
        jsonx_lib::write_string(out, name);
        out.push(':');
        jsonx_lib::write_string(out, text);
    }
}

/// MCP progress for one request: `notifications/progress` with the request's
/// progress token.
///
/// MCP requires that `progress` increase with every report, even when the
/// `total` is unknown, and that reports stop when the request completes (on
/// stdio, also once the peer cancelled it). A `Progress` holds both rules:
///
/// - [`report`](Progress::report) refuses a value that does not exceed the
///   last one sent, with [`Error::ProgressNotIncreasing`], and sends
///   nothing. For a handle bound to a request the history is the request's,
///   shared by every handle made for it (each call of `Context::progress` or
///   `Responder::progress`, and their clones); for one bound to nothing it is
///   the handle's, shared by its clones;
/// - one made by [`Context::progress`] or [`Responder::progress`] is bound to
///   its request: once the request is answered or cancelled (or the
///   connection closes), `report` refuses with [`Error::AlreadyAnswered`],
///   checked under the lock the response is written under, so no report can
///   follow the response on the wire. One made by [`Progress::new`] or
///   [`Progress::from_params`] knows no request and leaves that rule to the
///   caller.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Router};
///
/// let router = Router::new().deferred("tools/call", |_: &mut (), params, _cx, responder| {
///     let progress = responder.progress(&params);
///     std::thread::spawn(move || {
///         for step in 1..=2 {
///             if let Some(progress) = &progress {
///                 progress.report(f64::from(step), Some(2.0), Some("indexing")).ok();
///             }
///         }
///         responder.ok("done").ok();
///     });
/// });
/// let (tx, rx) = std::sync::mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, router, ());
/// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"_meta":{"progressToken":9}}}"#)?;
/// let first = rx.recv_timeout(std::time::Duration::from_secs(5)).ok();
/// assert_eq!(
///     first.as_deref(),
///     Some(r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":9,"progress":1,"total":2,"message":"indexing"}}"#),
/// );
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone)]
pub struct Progress {
    peer: Peer,
    token: Raw<'static>,
    scope: Scope,
    /// For a handle bound to no request, the value last sent, shared by the
    /// clones: held while a report is checked and sent, so concurrent reports
    /// reach the wire in order. A bound handle's history is its request's,
    /// kept with the request under the shared lock instead.
    last: Arc<Mutex<Option<f64>>>,
}

impl Progress {
    /// Progress for `token`, sent through `peer`, not bound to a request:
    /// reports must still increase, but nothing stops one after the request
    /// is answered. Prefer [`Context::progress`] or [`Responder::progress`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Progress, Raw, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// let progress = Progress::new(endpoint.peer(), Raw::new("\"t\"").map_err(|e| e.to_string())?);
    /// assert_eq!(progress.token().as_str(), "\"t\"");
    /// # Ok::<(), String>(())
    /// ```
    #[must_use]
    pub fn new(peer: Peer, token: Raw<'_>) -> Self {
        Self::scoped(peer, token, Scope::Free)
    }

    fn scoped(peer: Peer, token: Raw<'_>, scope: Scope) -> Self {
        Self {
            peer,
            token: token.into_owned(),
            scope,
            last: Arc::default(),
        }
    }

    /// Progress for the request whose params are `params`, if it carries a
    /// progress token (see [`Params::progress_token`]), not bound to the
    /// request (see [`Progress::new`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Progress, Router};
    ///
    /// let router = Router::new().request("work", |_: &mut (), params, cx| {
    ///     let progress = Progress::from_params(&params, cx.peer());
    ///     Ok(progress.is_some())
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"work","params":{"_meta":{"progressToken":"a"}}}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|r| r.ends_with(r#""result":true}"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn from_params(params: &Params<'_>, peer: Peer) -> Option<Self> {
        params.progress_token().map(|token| Self::new(peer, token))
    }

    /// The progress token.
    ///
    /// # Examples
    ///
    /// See [`Progress::new`].
    #[must_use]
    pub fn token(&self) -> &Raw<'static> {
        &self.token
    }

    /// Reports `progress` so far, out of `total` if it is known, with an
    /// optional human-readable `message`.
    ///
    /// # Errors
    ///
    /// - [`Error::Serialize`] for a number that is not finite (JSON has no
    ///   NaN or infinity);
    /// - [`Error::ProgressNotIncreasing`] for a `progress` that does not
    ///   exceed the last one sent;
    /// - [`Error::AlreadyAnswered`] for a handle bound to a request that was
    ///   answered or cancelled;
    /// - otherwise as [`Peer::notify`].
    ///
    /// Nothing is sent when it fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Error, Progress, Raw, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// let progress = Progress::new(endpoint.peer(), Raw::new("1").map_err(|e| Error::Serialize(e.to_string()))?);
    /// progress.report(0.5, None, None)?;
    /// assert_eq!(
    ///     rx.try_recv().ok().as_deref(),
    ///     Some(r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":1,"progress":0.5}}"#),
    /// );
    /// assert!(matches!(progress.report(f64::NAN, None, None), Err(Error::Serialize(_))));
    /// assert!(matches!(progress.report(0.5, None, None), Err(Error::ProgressNotIncreasing { .. })));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn report(
        &self,
        progress: f64,
        total: Option<f64>,
        message: Option<&str>,
    ) -> Result<(), Error> {
        finite("progress", progress)?;
        if let Some(total) = total {
            finite("total", total)?;
        }
        // A handle bound to a request has its history checked and kept with
        // the request, under the lock the report is sent under, so every
        // handle for that request shares it. One bound to nothing keeps its
        // own, shared by its clones, under this lock.
        let bound = !matches!(self.scope, Scope::Free);
        let mut last = (!bound).then(|| self.last.lock().unwrap_or_else(PoisonError::into_inner));
        if let Some(Some(last)) = last.as_deref() {
            if progress <= *last {
                return Err(Error::ProgressNotIncreasing {
                    last: *last,
                    next: progress,
                });
            }
        }
        // Encoded in this thread's reused buffer: no allocation per report.
        let fill = |out: &mut String| {
            out.push_str("{\"progressToken\":");
            out.push_str(self.token.as_str());
            push_number(out, "progress", progress);
            if let Some(total) = total {
                push_number(out, "total", total);
            }
            push_text(out, "message", message);
            out.push('}');
        };
        with_buffer(fill, |params| {
            self.peer.link().notify_in(
                &self.scope,
                bound.then_some(progress),
                "notifications/progress",
                Some(&Json::Raw(params)),
            )
        })?;
        if let Some(last) = last.as_deref_mut() {
            *last = Some(progress);
        }
        Ok(())
    }
}

impl Context<'_> {
    /// MCP progress for the request being handled, if it carries a progress
    /// token: a [`Progress`] bound to this request, which refuses to report
    /// once the request is answered (for a request answered by the handler's
    /// return value, as soon as the handler returns) or cancelled. `None`
    /// for a notification, or a request without a token.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Error, Progress, Router};
    ///
    /// let router = Router::new().request("work", |kept: &mut Option<Progress>, params, cx| {
    ///     let progress = cx.progress(&params);
    ///     if let Some(progress) = &progress {
    ///         progress.report(1.0, Some(1.0), None)?;
    ///     }
    ///     *kept = progress; // too late to use once the handler returns
    ///     Ok("done")
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, None);
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"work","params":{"_meta":{"progressToken":7}}}"#)?;
    /// let sent: Vec<String> = rx.try_iter().collect();
    /// assert!(sent[0].contains("notifications/progress"));
    /// assert_eq!(sent[1], r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#);
    /// let late = endpoint.state().as_ref().map(|p| p.report(2.0, None, None));
    /// assert!(matches!(late, Some(Err(Error::AlreadyAnswered))));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn progress(&self, params: &Params<'_>) -> Option<Progress> {
        let scope = self.scope()?;
        let token = params.progress_token()?;
        Some(Progress::scoped(self.peer(), token, scope))
    }
}

impl Responder {
    /// MCP progress for this deferred request, if `params` (its params)
    /// carry a progress token: a [`Progress`] bound to the request, which
    /// refuses to report once it is answered (through this responder, or by
    /// the connection) or cancelled by the peer.
    ///
    /// # Examples
    ///
    /// See [`Progress`].
    #[must_use]
    pub fn progress(&self, params: &Params<'_>) -> Option<Progress> {
        let token = params.progress_token()?;
        Some(Progress::scoped(self.peer(), token, self.scope()))
    }
}

impl fmt::Debug for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Progress")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}

/// LSP work-done progress for one token: `$/progress` reports of kind
/// `begin`, then any number of `report`, then `end`.
///
/// The token comes from the client's request (`workDoneToken`, see
/// [`Params::work_done_token`]) or from a `window/workDoneProgress/create`
/// request the server sent first.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Raw, Router, WorkDone};
///
/// let (tx, rx) = std::sync::mpsc::channel::<String>();
/// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
/// let work = WorkDone::new(endpoint.peer(), Raw::new("\"index\"").map_err(|e| rpc_io::Error::Serialize(e.to_string()))?);
/// work.begin("Indexing", false, None, Some(0))?;
/// work.report(Some("3/10 files"), Some(30))?;
/// work.end(Some("done"))?;
/// let sent: Vec<String> = rx.try_iter().collect();
/// assert_eq!(sent[0], r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"index","value":{"kind":"begin","title":"Indexing","cancellable":false,"percentage":0}}}"#);
/// assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"index","value":{"kind":"report","message":"3/10 files","percentage":30}}}"#);
/// assert_eq!(sent[2], r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"index","value":{"kind":"end","message":"done"}}}"#);
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone)]
pub struct WorkDone {
    peer: Peer,
    token: Raw<'static>,
}

impl WorkDone {
    /// Work-done progress for `token`, sent through `peer`.
    ///
    /// # Examples
    ///
    /// See [`WorkDone`].
    #[must_use]
    pub fn new(peer: Peer, token: Raw<'_>) -> Self {
        Self {
            peer,
            token: token.into_owned(),
        }
    }

    /// Work-done progress for the request whose params are `params`, if the
    /// client gave it a token.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router, WorkDone};
    ///
    /// let router = Router::new().request("workspace/symbol", |_: &mut (), params, cx| {
    ///     if let Some(work) = WorkDone::from_params(&params, cx.peer()) {
    ///         work.begin("Searching", false, None, None)?;
    ///         work.end(None)?;
    ///     }
    ///     Ok(rpc_io::jsonx::json!([]))
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(
    ///     br#"{"jsonrpc":"2.0","id":1,"method":"workspace/symbol","params":{"query":"","workDoneToken":"w"}}"#,
    /// )?;
    /// assert_eq!(rx.try_iter().count(), 3); // begin, end, the response
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn from_params(params: &Params<'_>, peer: Peer) -> Option<Self> {
        params.work_done_token().map(|token| Self::new(peer, token))
    }

    /// The token.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Raw, Router, WorkDone};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// let work = WorkDone::new(endpoint.peer(), Raw::new("2").map_err(|e| e.to_string())?);
    /// assert_eq!(work.token().as_str(), "2");
    /// # Ok::<(), String>(())
    /// ```
    #[must_use]
    pub fn token(&self) -> &Raw<'static> {
        &self.token
    }

    /// Starts the work: a `begin` report with its `title`, whether the client
    /// may cancel it, and an optional first message and percentage (0 to
    /// 100).
    ///
    /// # Errors
    ///
    /// As [`Peer::notify`].
    ///
    /// # Examples
    ///
    /// See [`WorkDone`].
    pub fn begin(
        &self,
        title: &str,
        cancellable: bool,
        message: Option<&str>,
        percentage: Option<u32>,
    ) -> Result<(), Error> {
        self.send(|value| {
            value.push_str("{\"kind\":\"begin\"");
            push_text(value, "title", Some(title));
            value.push_str(if cancellable {
                ",\"cancellable\":true"
            } else {
                ",\"cancellable\":false"
            });
            push_text(value, "message", message);
            push_percentage(value, percentage);
        })
    }

    /// Reports progress: an optional message and percentage (0 to 100; LSP
    /// clients show a percentage that goes down as it was).
    ///
    /// # Errors
    ///
    /// As [`Peer::notify`].
    ///
    /// # Examples
    ///
    /// See [`WorkDone`].
    pub fn report(&self, message: Option<&str>, percentage: Option<u32>) -> Result<(), Error> {
        self.send(|value| {
            value.push_str("{\"kind\":\"report\"");
            push_text(value, "message", message);
            push_percentage(value, percentage);
        })
    }

    /// Ends the work, with an optional final message.
    ///
    /// # Errors
    ///
    /// As [`Peer::notify`].
    ///
    /// # Examples
    ///
    /// See [`WorkDone`].
    pub fn end(&self, message: Option<&str>) -> Result<(), Error> {
        self.send(|value| {
            value.push_str("{\"kind\":\"end\"");
            push_text(value, "message", message);
        })
    }

    /// Sends `$/progress` with `value` (an object missing its `}`).
    /// Sends `$/progress` for the token, with the `value` object `members`
    /// opens and fills (its closing brace is added here), encoded in this
    /// thread's reused buffer.
    fn send(&self, members: impl FnOnce(&mut String)) -> Result<(), Error> {
        let fill = |out: &mut String| {
            out.push_str("{\"token\":");
            out.push_str(self.token.as_str());
            out.push_str(",\"value\":");
            members(out);
            out.push_str("}}");
        };
        with_buffer(fill, |params| self.peer.notify("$/progress", params))
    }
}

/// Appends `"percentage":<n>`, at most 100, when there is one.
fn push_percentage(out: &mut String, percentage: Option<u32>) {
    if let Some(percentage) = percentage {
        out.push_str(",\"percentage\":");
        crate::text::push_u64(out, u64::from(percentage.min(100)));
    }
}

impl fmt::Debug for WorkDone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkDone")
            .field("token", &self.token)
            .finish_non_exhaustive()
    }
}
