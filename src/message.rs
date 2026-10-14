//! The JSON-RPC 2.0 message model: [`decode`] a message body into
//! [`Message`]s, and write them back.
//!
//! Decoding is one pass of `jsonx_lib`'s pull [`Reader`] over the body: the
//! envelope members are read as raw slices (`read_raw`), so `params`,
//! `result`, and error `data` are validated but never decoded or copied, and
//! strings without escapes (methods, string ids) borrow from the body.

use alloc::{borrow::Cow, format, string::String, vec::Vec};
use core::fmt;

use jsonx_lib::{Event, Number, Parser, Reader};

use crate::{
    error_object::ErrorObject,
    id::Id,
    limits::Limits,
    raw::{Json, Raw},
};

/// A request: a call that expects exactly one response.
///
/// The fields are public to read and to change; the struct is
/// `#[non_exhaustive]`, so build one with [`Request::new`] (a later version
/// may keep more of the envelope).
///
/// # Examples
///
/// ```
/// use rpc_io::{Message, Raw, Request};
///
/// let request = Request::new(1, "sum", Some(Raw::new("[1, 2]")?));
/// assert_eq!(
///     Message::Request(request).to_string(),
///     r#"{"jsonrpc":"2.0","id":1,"method":"sum","params":[1, 2]}"#,
/// );
/// # Ok::<(), jsonx_lib::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Request<'a> {
    /// The id the response must carry.
    pub id: Id<'a>,
    /// The method to call.
    pub method: Cow<'a, str>,
    /// The parameters: an array or an object, or `None` when absent.
    pub params: Option<Raw<'a>>,
}

/// A notification: a call that gets no response.
///
/// The fields are public to read and to change; the struct is
/// `#[non_exhaustive]`, so build one with [`Notification::new`].
///
/// # Examples
///
/// ```
/// use rpc_io::{Message, Notification};
///
/// let n = Notification::new("exit", None);
/// assert_eq!(Message::Notification(n).to_string(), r#"{"jsonrpc":"2.0","method":"exit"}"#);
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Notification<'a> {
    /// The method to call.
    pub method: Cow<'a, str>,
    /// The parameters: an array or an object, or `None` when absent.
    pub params: Option<Raw<'a>>,
}

/// A response: the result of a request, or why it failed.
///
/// `id` is `None` only for an error answering a message whose id could not be
/// read (it is written as `"id": null`).
///
/// The fields are public to read and to change; the struct is
/// `#[non_exhaustive]`, so build one with [`Response::ok`],
/// [`Response::err`], or [`Response::new`].
///
/// # Examples
///
/// ```
/// use rpc_io::{ErrorObject, Message, Raw, Response};
///
/// let ok = Response::ok(4, Raw::new("true")?);
/// assert_eq!(Message::Response(ok).to_string(), r#"{"jsonrpc":"2.0","id":4,"result":true}"#);
///
/// let failed = Response::err(None, ErrorObject::parse_error("bad JSON"));
/// assert_eq!(
///     Message::Response(failed).to_string(),
///     r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad JSON"}}"#,
/// );
/// # Ok::<(), jsonx_lib::Error>(())
/// ```
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub struct Response<'a> {
    /// The id of the request answered, `None` for `null`.
    pub id: Option<Id<'a>>,
    /// The `result` member, or the `error` member.
    pub result: Result<Raw<'a>, ErrorObject>,
}

impl<'a> Request<'a> {
    /// A request for `method` under `id`, with `params` (an array or an
    /// object; `None` for absent).
    ///
    /// # Examples
    ///
    /// ```
    /// let request = rpc_io::Request::new("req-1", "shutdown", None);
    /// assert_eq!(request.id.as_str(), Some("req-1"));
    /// assert_eq!(request.method, "shutdown");
    /// ```
    #[must_use]
    pub fn new(
        id: impl Into<Id<'a>>,
        method: impl Into<Cow<'a, str>>,
        params: Option<Raw<'a>>,
    ) -> Self {
        Self {
            id: id.into(),
            method: method.into(),
            params,
        }
    }
}

impl<'a> Notification<'a> {
    /// A notification for `method`, with `params` (an array or an object;
    /// `None` for absent).
    ///
    /// # Examples
    ///
    /// ```
    /// let n = rpc_io::Notification::new("initialized", Some(rpc_io::Raw::new("{}")?));
    /// assert_eq!(n.params.map(|p| p.as_str().to_owned()).as_deref(), Some("{}"));
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    #[must_use]
    pub fn new(method: impl Into<Cow<'a, str>>, params: Option<Raw<'a>>) -> Self {
        Self {
            method: method.into(),
            params,
        }
    }
}

impl<'a> Response<'a> {
    /// A response under `id` (`None` for `null`) with `result`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Raw, Response};
    ///
    /// let response = Response::new(Some(7.into()), Ok(Raw::NULL));
    /// assert!(response.result.is_ok());
    /// ```
    #[must_use]
    pub const fn new(id: Option<Id<'a>>, result: Result<Raw<'a>, ErrorObject>) -> Self {
        Self { id, result }
    }

    /// A successful response under `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// let ok = rpc_io::Response::ok("a", rpc_io::Raw::new("[1]")?);
    /// assert_eq!(ok.id.and_then(|id| id.as_str().map(str::to_owned)).as_deref(), Some("a"));
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    #[must_use]
    pub fn ok(id: impl Into<Id<'a>>, result: Raw<'a>) -> Self {
        Self {
            id: Some(id.into()),
            result: Ok(result),
        }
    }

    /// An error response under `id` (`None` for `null`, for a message whose
    /// id could not be read).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorObject, Id, Response};
    ///
    /// let failed = Response::err(Some(Id::from(3)), ErrorObject::method_not_found("x"));
    /// assert!(failed.result.is_err());
    /// ```
    #[must_use]
    pub const fn err(id: Option<Id<'a>>, error: ErrorObject) -> Self {
        Self {
            id,
            result: Err(error),
        }
    }
}

/// One JSON-RPC 2.0 message.
///
/// # Examples
///
/// ```
/// use rpc_io::{decode, Incoming, Limits, Message};
///
/// let body = br#"{"jsonrpc": "2.0", "id": "a", "method": "ping"}"#;
/// let Incoming::Single(Ok(Message::Request(request))) = decode(body, &Limits::new()) else {
///     panic!("expected a request");
/// };
/// assert_eq!(request.method, "ping");
/// assert_eq!(request.id.as_str(), Some("a"));
/// assert!(request.params.is_none());
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Message<'a> {
    /// A request.
    Request(Request<'a>),
    /// A notification.
    Notification(Notification<'a>),
    /// A response.
    Response(Response<'a>),
}

impl<'a> Message<'a> {
    /// The id: a request's, or a response's (`None` for `null`). `None` for a
    /// notification.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Message, Notification};
    ///
    /// let n = Message::Notification(Notification::new("x", None));
    /// assert!(n.id().is_none());
    /// ```
    #[must_use]
    pub fn id(&self) -> Option<&Id<'a>> {
        match self {
            Message::Request(r) => Some(&r.id),
            Message::Notification(_) => None,
            Message::Response(r) => r.id.as_ref(),
        }
    }

    /// The method of a request or notification.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Message, Notification};
    ///
    /// let n = Message::Notification(Notification::new("x", None));
    /// assert_eq!(n.method(), Some("x"));
    /// ```
    #[must_use]
    pub fn method(&self) -> Option<&str> {
        match self {
            Message::Request(r) => Some(&r.method),
            Message::Notification(n) => Some(&n.method),
            Message::Response(_) => None,
        }
    }

    /// Appends the message as compact JSON.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Message, Notification};
    ///
    /// let mut out = String::new();
    /// Message::Notification(Notification::new("x", None)).write(&mut out);
    /// assert_eq!(out, r#"{"jsonrpc":"2.0","method":"x"}"#);
    /// ```
    pub fn write(&self, out: &mut String) {
        match self {
            Message::Request(r) => {
                let params = r.params.as_ref().map(|p| Json::Raw(p.reborrow()));
                write_call(out, Some(&r.id), &r.method, params.as_ref());
            }
            Message::Notification(n) => {
                let params = n.params.as_ref().map(|p| Json::Raw(p.reborrow()));
                write_call(out, None, &n.method, params.as_ref());
            }
            Message::Response(r) => {
                let result = match &r.result {
                    Ok(raw) => Ok(Json::Raw(raw.reborrow())),
                    Err(error) => Err(error),
                };
                write_response(out, r.id.as_ref(), result.as_ref().map_err(|e| *e));
            }
        }
    }

    /// Copies everything borrowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Message, Notification};
    ///
    /// let text = String::from("x");
    /// let owned: Message<'static> =
    ///     Message::Notification(Notification::new(text.as_str(), None)).into_owned();
    /// drop(text);
    /// assert_eq!(owned.method(), Some("x"));
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Message<'static> {
        match self {
            Message::Request(r) => Message::Request(Request {
                id: r.id.into_owned(),
                method: Cow::Owned(r.method.into_owned()),
                params: r.params.map(Raw::into_owned),
            }),
            Message::Notification(n) => Message::Notification(Notification {
                method: Cow::Owned(n.method.into_owned()),
                params: n.params.map(Raw::into_owned),
            }),
            Message::Response(r) => Message::Response(Response {
                id: r.id.map(Id::into_owned),
                result: r.result.map(Raw::into_owned),
            }),
        }
    }
}

impl fmt::Display for Message<'_> {
    /// Writes the message as compact JSON.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::new();
        self.write(&mut out);
        f.write_str(&out)
    }
}

/// Appends `messages` as one batch: a JSON array of the messages.
///
/// An empty slice writes `[]`, which JSON-RPC 2.0 forbids sending; the caller
/// decides what to send.
///
/// # Examples
///
/// ```
/// use rpc_io::{write_batch, Message, Notification};
///
/// let ping = Message::Notification(Notification::new("ping", None));
/// let mut out = String::new();
/// write_batch(&mut out, &[ping.clone(), ping]);
/// assert_eq!(out, r#"[{"jsonrpc":"2.0","method":"ping"},{"jsonrpc":"2.0","method":"ping"}]"#);
/// ```
pub fn write_batch(out: &mut String, messages: &[Message<'_>]) {
    out.push('[');
    for (i, message) in messages.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        message.write(out);
    }
    out.push(']');
}

/// Writes a request (`id` given) or a notification.
pub(crate) fn write_call(
    out: &mut String,
    id: Option<&Id<'_>>,
    method: &str,
    params: Option<&Json<'_>>,
) {
    out.push_str("{\"jsonrpc\":\"2.0\",");
    if let Some(id) = id {
        out.push_str("\"id\":");
        id.write(out);
        out.push(',');
    }
    write_method_params(out, method, params);
}

/// Writes a request with a numeric id the crate issued.
#[cfg(feature = "std")]
pub(crate) fn write_numbered_call(
    out: &mut String,
    id: u64,
    method: &str,
    params: Option<&Json<'_>>,
) {
    out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
    crate::text::push_u64(out, id);
    out.push(',');
    write_method_params(out, method, params);
}

fn write_method_params(out: &mut String, method: &str, params: Option<&Json<'_>>) {
    out.push_str("\"method\":");
    jsonx_lib::write_string(out, method);
    if let Some(params) = params {
        out.push_str(",\"params\":");
        params.write(out);
    }
    out.push('}');
}

/// Writes a response.
pub(crate) fn write_response(
    out: &mut String,
    id: Option<&Id<'_>>,
    result: Result<&Json<'_>, &ErrorObject>,
) {
    out.push_str("{\"jsonrpc\":\"2.0\",\"id\":");
    match id {
        Some(id) => id.write(out),
        None => out.push_str("null"),
    }
    match result {
        Ok(value) => {
            out.push_str(",\"result\":");
            value.write(out);
        }
        Err(error) => {
            out.push_str(",\"error\":");
            error.write(out);
        }
    }
    out.push('}');
}

/// A message that is not a valid JSON-RPC 2.0 message, and how to answer it.
///
/// Malformed messages come in two kinds:
///
/// - **Request-like**: anything with a `method` member, and anything that is
///   neither a request nor a response (`{}`, `[1]`'s `1`, invalid JSON). It is
///   answered with the [`error`](Invalid::error), under the message's own id
///   when it had a `method` and a valid `id` (so the sender can match the
///   error to its request), and under `null` otherwise.
/// - **Response-like**: no `method` but a `result` or an `error`: a broken
///   answer to one of *our* requests. It is never answered (an error under its
///   id would read, to the peer, as an answer to one of the peer's own
///   requests); its [`id`](Invalid::id), when valid, names the request it was
///   meant to answer.
///
/// # Examples
///
/// ```
/// use rpc_io::{decode, ErrorCode, Incoming, Limits};
///
/// // `params` must be an array or an object: answered under the request's id.
/// let body = br#"{"jsonrpc": "2.0", "id": 9, "method": "m", "params": 5}"#;
/// let Incoming::Single(Err(invalid)) = decode(body, &Limits::new()) else { panic!() };
/// assert!(!invalid.is_response());
/// assert_eq!(invalid.id().and_then(|id| id.as_i64()), Some(9));
/// assert_eq!(invalid.error().code(), ErrorCode::INVALID_REQUEST);
/// let reply = invalid.reply().map(|r| rpc_io::Message::Response(r).to_string());
/// assert!(reply.is_some_and(|r| r.starts_with(r#"{"jsonrpc":"2.0","id":9,"error":{"code":-32600"#)));
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct Invalid<'a> {
    id: Option<Id<'a>>,
    response: bool,
    error: ErrorObject,
}

impl<'a> Invalid<'a> {
    /// For a request-like message, the id to answer under (`None` for
    /// `null`); for a response-like one, the id of the request it was meant to
    /// answer, if readable.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, Incoming, Limits};
    ///
    /// let Incoming::Single(Err(invalid)) = decode(b"{}", &Limits::new()) else { panic!() };
    /// assert!(invalid.id().is_none());
    /// ```
    #[must_use]
    pub fn id(&self) -> Option<&Id<'a>> {
        self.id.as_ref()
    }

    /// Whether this is a malformed response, which must not be answered.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, Incoming, Limits};
    ///
    /// let body = br#"{"jsonrpc": "2.0", "id": 1, "result": 1, "error": {"code": 1, "message": ""}}"#;
    /// let Incoming::Single(Err(invalid)) = decode(body, &Limits::new()) else { panic!() };
    /// assert!(invalid.is_response());
    /// assert!(invalid.reply().is_none());
    /// ```
    #[must_use]
    pub const fn is_response(&self) -> bool {
        self.response
    }

    /// What is wrong, as the error to answer with: `PARSE_ERROR` for invalid
    /// JSON, `INVALID_REQUEST` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, ErrorCode, Incoming, Limits};
    ///
    /// let Incoming::Single(Err(invalid)) = decode(b"{", &Limits::new()) else { panic!() };
    /// assert_eq!(invalid.error().code(), ErrorCode::PARSE_ERROR);
    /// ```
    #[must_use]
    pub const fn error(&self) -> &ErrorObject {
        &self.error
    }

    /// The error response to send, or `None` for a response-like message.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, Incoming, Limits, Message};
    ///
    /// let Incoming::Single(Err(invalid)) = decode(b"[]", &Limits::new()) else { panic!() };
    /// let reply = invalid.reply().map(|r| Message::Response(r).to_string());
    /// assert_eq!(
    ///     reply.as_deref(),
    ///     Some(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"an empty batch is not a valid request"}}"#),
    /// );
    /// ```
    #[must_use]
    pub fn reply(&self) -> Option<Response<'a>> {
        (!self.response).then(|| Response {
            id: self.id.clone(),
            result: Err(self.error.clone()),
        })
    }

    /// Copies everything borrowed.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, Incoming, Limits};
    ///
    /// let Incoming::Single(Err(invalid)) = decode(b"1", &Limits::new()) else { panic!() };
    /// let owned: rpc_io::Invalid<'static> = invalid.into_owned();
    /// assert!(owned.id().is_none());
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Invalid<'static> {
        Invalid {
            id: self.id.map(Id::into_owned),
            response: self.response,
            error: self.error,
        }
    }

    fn request(id: Option<Id<'a>>, error: ErrorObject) -> Self {
        Self {
            id,
            response: false,
            error,
        }
    }
}

/// One decoded message, or why it is not one.
pub type Entry<'a> = Result<Message<'a>, Invalid<'a>>;

/// A decoded message body: one message, or a batch.
///
/// A body that is not valid JSON, an empty batch, a batch over
/// [`Limits::max_batch`], and any batch while batches are off are each a
/// single [`Invalid`] (answered with one error, as JSON-RPC 2.0 prescribes),
/// never a batch.
///
/// # Examples
///
/// ```
/// use rpc_io::{decode, Incoming, Limits};
///
/// let body = br#"[{"jsonrpc": "2.0", "method": "a"}, 7]"#;
/// let Incoming::Batch(entries) = decode(body, &Limits::new()) else { panic!() };
/// assert_eq!(entries.len(), 2);
/// assert!(entries[0].is_ok());
/// assert!(entries[1].is_err()); // `7` is not a message
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Incoming<'a> {
    /// A single message (or a body that could not be read as any message).
    Single(Entry<'a>),
    /// A batch: one entry per element, in order. Never empty.
    Batch(Vec<Entry<'a>>),
}

/// Decodes one message body: a single message or a batch.
///
/// The body is validated as JSON in full before anything is returned, so a
/// batch with a syntax error anywhere is a single `PARSE_ERROR`, as JSON-RPC
/// 2.0 requires; with two exceptions, both refused early so that a batch's
/// size cannot drive the work done: a batch is refused with one
/// `INVALID_REQUEST` as soon as it passes [`Limits::max_batch`] (the rest is
/// not read, so a syntax error after that point is not reported), and at its
/// `[` when batches are off (`max_batch` 0). Envelope rules, beyond the
/// specification's:
///
/// - `"jsonrpc"` must be exactly `"2.0"`; unknown members are ignored; a
///   repeated envelope member makes the message invalid (two readers could
///   otherwise disagree about which one counts).
/// - Request ids must be strings or integers within `i64` (see [`Id`]).
/// - `"params": null` reads as absent params; any other non-structured value is
///   invalid.
/// - A message over a JSON budget (depth, string length, value count) is
///   `INVALID_REQUEST`, answered under its id when the id and method were read
///   before the budget ran out.
///
/// Nothing is allocated for `params` or `result`: they are slices of `body`.
///
/// # Examples
///
/// ```
/// use rpc_io::{decode, Incoming, Limits, Message};
///
/// let body = br#"{"jsonrpc": "2.0", "id": 1, "method": "add", "params": [2, 3]}"#;
/// let Incoming::Single(Ok(Message::Request(request))) = decode(body, &Limits::new()) else {
///     panic!();
/// };
/// let params = request.params.as_ref().map(|p| p.as_str());
/// assert_eq!(params, Some("[2, 3]")); // a slice of `body`, not a copy
/// ```
#[must_use]
pub fn decode<'a>(body: &'a [u8], limits: &Limits) -> Incoming<'a> {
    let parser = limits.parser();
    let mut reader = match parser.reader_bytes(body) {
        Ok(reader) => reader,
        Err(error) => return Incoming::Single(Err(unreadable(Fail::Json(error), None))),
    };
    let first = match reader.next_event() {
        Ok(Some(event)) => event,
        Ok(None) => return Incoming::Single(Err(unreadable(Fail::Shape, None))),
        Err(error) => return Incoming::Single(Err(unreadable(Fail::Json(error), None))),
    };
    match first {
        Event::StartObject => {
            let mut fields = Fields::default();
            let read = read_members(&mut reader, &mut fields)
                .and_then(|()| reader.finish().map_err(Fail::Json));
            match read {
                Ok(()) => Incoming::Single(classify(fields, &parser)),
                Err(fail) => {
                    let salvaged = fields.salvage();
                    Incoming::Single(Err(unreadable(fail, salvaged)))
                }
            }
        }
        Event::StartArray => decode_batch(reader, &parser, limits.max_batch),
        _ => match reader.finish() {
            Ok(()) => Incoming::Single(Err(Invalid::request(
                None,
                ErrorObject::invalid_request("a message must be an object or an array"),
            ))),
            Err(error) => Incoming::Single(Err(unreadable(Fail::Json(error), None))),
        },
    }
}

/// Decodes the elements of a batch, the reader just past its `[`.
fn decode_batch<'a>(mut reader: Reader<'a>, parser: &Parser, max: usize) -> Incoming<'a> {
    if max == 0 {
        // Batches are off: refused at the `[`, unread.
        return Incoming::Single(Err(Invalid::request(
            None,
            ErrorObject::invalid_request("batches are not accepted"),
        )));
    }
    let mut entries = Vec::new();
    loop {
        let event = match reader.next_event() {
            Ok(Some(event)) => event,
            Ok(None) => return Incoming::Single(Err(unreadable(Fail::Shape, None))),
            Err(error) => return Incoming::Single(Err(unreadable(Fail::Json(error), None))),
        };
        if matches!(event, Event::EndArray) {
            break;
        }
        if entries.len() >= max {
            // Refused before the rest is read: the batch is answered with one
            // error, and its size cannot drive the work done.
            let message = format!("a batch may hold at most {max} messages");
            return Incoming::Single(Err(Invalid::request(
                None,
                ErrorObject::invalid_request(message),
            )));
        }
        let entry = match event {
            Event::StartObject => {
                let mut fields = Fields::default();
                if let Err(fail) = read_members(&mut reader, &mut fields) {
                    return Incoming::Single(Err(unreadable(fail, None)));
                }
                classify(fields, parser)
            }
            Event::StartArray => {
                if let Err(fail) = skip_container(&mut reader) {
                    return Incoming::Single(Err(unreadable(fail, None)));
                }
                Err(not_a_message())
            }
            Event::EndObject | Event::Key(_) => {
                return Incoming::Single(Err(unreadable(Fail::Shape, None)));
            }
            _ => Err(not_a_message()),
        };
        entries.push(entry);
    }
    if let Err(error) = reader.finish() {
        return Incoming::Single(Err(unreadable(Fail::Json(error), None)));
    }
    if entries.is_empty() {
        return Incoming::Single(Err(Invalid::request(
            None,
            ErrorObject::invalid_request("an empty batch is not a valid request"),
        )));
    }
    Incoming::Batch(entries)
}

fn not_a_message() -> Invalid<'static> {
    Invalid::request(
        None,
        ErrorObject::invalid_request("a batch element must be a message object"),
    )
}

/// Consumes the rest of a container whose opening event was just read.
/// Iterative: the depth is a counter, bounded by the reader's depth budget.
fn skip_container(reader: &mut Reader<'_>) -> Result<(), Fail> {
    let mut depth = 1usize;
    while depth > 0 {
        match reader.next_event().map_err(Fail::Json)? {
            Some(Event::StartArray | Event::StartObject) => depth += 1,
            Some(Event::EndArray | Event::EndObject) => depth -= 1,
            Some(_) => {}
            None => return Err(Fail::Shape),
        }
    }
    Ok(())
}

/// Why a body could not be read.
enum Fail {
    /// The JSON parser refused it: a syntax error or an exceeded budget.
    Json(jsonx_lib::Error),
    /// The reader produced an event its grammar rules out at this point.
    Shape,
}

/// The `Invalid` for a body that could not be read.
fn unreadable(fail: Fail, salvaged: Option<Id<'_>>) -> Invalid<'_> {
    match fail {
        Fail::Json(error) if error.kind().is_budget() => Invalid::request(
            salvaged,
            ErrorObject::invalid_request(format!("the message exceeds a limit: {error}")),
        ),
        Fail::Json(error) => Invalid::request(
            None,
            ErrorObject::parse_error(format!("invalid JSON: {error}")),
        ),
        Fail::Shape => Invalid::request(None, ErrorObject::parse_error("invalid JSON")),
    }
}

/// The envelope members of one message object, as raw slices.
#[derive(Default)]
struct Fields<'a> {
    jsonrpc: Option<&'a str>,
    id: Option<&'a str>,
    method: Option<&'a str>,
    params: Option<&'a str>,
    result: Option<&'a str>,
    error: Option<&'a str>,
    /// The first envelope member that appeared twice.
    duplicate: Option<&'static str>,
    /// Whether `id` appeared more than once (tracked apart from `duplicate`,
    /// which keeps only the first repeat): such an id answers nothing.
    id_repeated: bool,
}

impl<'a> Fields<'a> {
    /// The id to answer an unreadable message under: its own if it was read,
    /// valid, unrepeated, and the message has a method.
    fn salvage(&self) -> Option<Id<'a>> {
        if self.method.is_none() || self.id_repeated {
            return None;
        }
        match self.id.map(read_id) {
            Some(IdValue::Valid(id)) => Some(id),
            _ => None,
        }
    }
}

/// Reads an object's members up to its `}`, keeping the envelope's as raw
/// slices and skipping the rest.
fn read_members<'a>(reader: &mut Reader<'a>, fields: &mut Fields<'a>) -> Result<(), Fail> {
    loop {
        let key = match reader.next_event().map_err(Fail::Json)? {
            Some(Event::Key(key)) => key,
            Some(Event::EndObject) => return Ok(()),
            _ => return Err(Fail::Shape),
        };
        let raw = reader.read_raw().map_err(Fail::Json)?.ok_or(Fail::Shape)?;
        let (slot, name) = match &*key {
            "jsonrpc" => (&mut fields.jsonrpc, "jsonrpc"),
            "id" => (&mut fields.id, "id"),
            "method" => (&mut fields.method, "method"),
            "params" => (&mut fields.params, "params"),
            "result" => (&mut fields.result, "result"),
            "error" => (&mut fields.error, "error"),
            _ => continue,
        };
        if slot.is_some() {
            let _first = fields.duplicate.get_or_insert(name);
            fields.id_repeated |= name == "id";
        } else {
            *slot = Some(raw);
        }
    }
}

/// What an `id` member holds.
enum IdValue<'a> {
    Valid(Id<'a>),
    Null,
    Invalid,
}

fn read_id(raw: &str) -> IdValue<'_> {
    match raw.as_bytes().first() {
        Some(b'"') => read_string(raw).map_or(IdValue::Invalid, |s| IdValue::Valid(Id::String(s))),
        Some(b'n') => IdValue::Null,
        Some(b'-' | b'0'..=b'9') => Number::from_text(raw)
            .and_then(|n| n.as_i64())
            .map_or(IdValue::Invalid, |n| IdValue::Valid(Id::Number(n))),
        _ => IdValue::Invalid,
    }
}

/// The id in member `member` of a params object (the target of a cancel
/// notification); `None` when absent, repeated, or not a valid id.
#[cfg(feature = "std")]
pub(crate) fn read_member_id<'a>(params: &'a str, member: &str, parser: &Parser) -> Option<Id<'a>> {
    match read_id(read_member(params, member, parser)?) {
        IdValue::Valid(id) => Some(id),
        IdValue::Null | IdValue::Invalid => None,
    }
}

/// The raw value of member `member` of the object `object`; `None` when
/// `object` is not an object, or the member is absent or repeated (two
/// values: acting on either could be wrong).
#[cfg(feature = "std")]
pub(crate) fn read_member<'a>(object: &'a str, member: &str, parser: &Parser) -> Option<&'a str> {
    let mut reader = parser.reader(object);
    if !matches!(reader.next_event(), Ok(Some(Event::StartObject))) {
        return None;
    }
    let mut found = None;
    loop {
        match reader.next_event() {
            Ok(Some(Event::Key(key))) => {
                let raw = reader.read_raw().ok()??;
                if *key == *member && found.replace(raw).is_some() {
                    return None;
                }
            }
            Ok(Some(Event::EndObject)) => return found,
            _ => return None,
        }
    }
}

/// Whether `raw` (validated JSON) is a string or an integer within `i64`:
/// what JSON-RPC ids and progress tokens may be.
#[cfg(feature = "std")]
pub(crate) fn is_token(raw: &str) -> bool {
    matches!(read_id(raw), IdValue::Valid(_))
}

/// The text of a raw JSON string, borrowed when it has no escapes; `None` if
/// `raw` is not a string.
fn read_string(raw: &str) -> Option<Cow<'_, str>> {
    let inner = raw.strip_prefix('"')?.strip_suffix('"')?;
    if !inner.contains('\\') {
        // Validated JSON: with no backslash, the text between the quotes is
        // the string itself.
        return Some(Cow::Borrowed(inner));
    }
    match Reader::new(raw).next_event() {
        Ok(Some(Event::String(s))) => Some(s),
        _ => None,
    }
}

/// Sorts a fully read message object into a message or an `Invalid`.
fn classify<'a>(fields: Fields<'a>, parser: &Parser) -> Entry<'a> {
    let id = fields.id.map(read_id);
    let id_repeated = fields.id_repeated;
    let valid_id = match &id {
        Some(IdValue::Valid(id)) if !id_repeated => Some(id.clone()),
        _ => None,
    };
    let version_ok = fields
        .jsonrpc
        .is_some_and(|v| v == "\"2.0\"" || read_string(v).is_some_and(|s| s == "2.0"));

    if let Some(method) = fields.method {
        let fail = |message: &'static str| {
            Err(Invalid::request(
                valid_id.clone(),
                ErrorObject::invalid_request(message),
            ))
        };
        if let Some(name) = fields.duplicate {
            return Err(Invalid::request(
                valid_id.clone(),
                ErrorObject::invalid_request(format!("the member `{name}` appears more than once")),
            ));
        }
        if !version_ok {
            return fail("`jsonrpc` must be \"2.0\"");
        }
        let Some(method) = read_string(method) else {
            return fail("`method` must be a string");
        };
        if fields.result.is_some() || fields.error.is_some() {
            return fail("a request cannot have a `result` or an `error`");
        }
        let params = match fields.params {
            None | Some("null") => None,
            Some(p) if p.starts_with(['[', '{']) => Some(Raw::trusted(p)),
            Some(_) => return fail("`params` must be an array or an object"),
        };
        return match id {
            None => Ok(Message::Notification(Notification { method, params })),
            Some(IdValue::Valid(id)) => Ok(Message::Request(Request { id, method, params })),
            Some(IdValue::Null) => fail("a request id must not be null"),
            Some(IdValue::Invalid) => fail("a request id must be a string or an integer"),
        };
    }

    if fields.result.is_none() && fields.error.is_none() {
        return Err(Invalid::request(
            None,
            ErrorObject::invalid_request("a message needs a `method`, or a `result` or an `error`"),
        ));
    }

    // A response.
    let fail = |message: Cow<'static, str>| {
        Err(Invalid {
            id: valid_id.clone(),
            response: true,
            error: ErrorObject::invalid_request(message),
        })
    };
    if let Some(name) = fields.duplicate {
        return fail(format!("the member `{name}` appears more than once").into());
    }
    if !version_ok {
        return fail("`jsonrpc` must be \"2.0\"".into());
    }
    let result = match (fields.result, fields.error) {
        (Some(_), Some(_)) => {
            return fail("a response cannot have both `result` and `error`".into());
        }
        (Some(result), None) => Ok(Raw::trusted(result)),
        (None, Some(error)) => match read_error(error, parser) {
            Ok(error) => Err(error),
            Err(why) => return fail(format!("malformed `error`: {why}").into()),
        },
        (None, None) => return fail("a response needs a `result` or an `error`".into()),
    };
    match id {
        None => fail("a response needs an `id`".into()),
        Some(IdValue::Invalid) => {
            fail("a response id must be a string, an integer, or null".into())
        }
        Some(IdValue::Null) if result.is_ok() => {
            fail("a successful response needs a non-null `id`".into())
        }
        Some(IdValue::Null) => Ok(Message::Response(Response { id: None, result })),
        Some(IdValue::Valid(id)) => Ok(Message::Response(Response {
            id: Some(id),
            result,
        })),
    }
}

/// Reads a response's `error` member strictly: an object with an integer
/// `code`, a string `message`, and optional `data`.
fn read_error(raw: &str, parser: &Parser) -> Result<ErrorObject, &'static str> {
    const NOT_AN_OBJECT: &str = "it must be an object with `code` and `message`";
    let mut reader = parser.reader(raw);
    if !matches!(reader.next_event(), Ok(Some(Event::StartObject))) {
        return Err(NOT_AN_OBJECT);
    }
    let (mut code, mut message, mut data) = (None, None, None);
    loop {
        let key = match reader.next_event() {
            Ok(Some(Event::Key(key))) => key,
            Ok(Some(Event::EndObject)) => break,
            _ => return Err(NOT_AN_OBJECT),
        };
        let Ok(Some(value)) = reader.read_raw() else {
            return Err(NOT_AN_OBJECT);
        };
        let slot = match &*key {
            "code" => &mut code,
            "message" => &mut message,
            "data" => &mut data,
            _ => continue,
        };
        if slot.replace(value).is_some() {
            return Err("a member appears more than once");
        }
    }
    let code = code
        .and_then(Number::from_text)
        .and_then(|n| n.as_i64())
        .ok_or("`code` must be an integer")?;
    let message = message
        .and_then(read_string)
        .ok_or("`message` must be a string")?;
    let data = data.map(|d| Raw::trusted(d).into_owned());
    Ok(ErrorObject::from_parts(code, clip_owned(message), data))
}

/// The peer's error message, clipped to a bounded length for the crate's own
/// error values; the full text is not needed to act on an error.
fn clip_owned(message: Cow<'_, str>) -> String {
    const MAX: usize = 4096;
    if message.len() <= MAX {
        return message.into_owned();
    }
    let mut end = MAX;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    String::from(&message[..end])
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::string::ToString;

    use super::*;
    use crate::ErrorCode;

    fn single(body: &str) -> Entry<'_> {
        match decode(body.as_bytes(), &Limits::new()) {
            Incoming::Single(entry) => entry,
            Incoming::Batch(_) => panic!("unexpected batch"),
        }
    }

    fn invalid(body: &str) -> Invalid<'_> {
        match single(body) {
            Err(invalid) => invalid,
            Ok(message) => panic!("expected invalid, got {message}"),
        }
    }

    #[test]
    fn test_request_and_notification() {
        let Ok(Message::Request(r)) =
            single(r#"{"jsonrpc":"2.0","id":-5,"method":"m","params":{"a":1}}"#)
        else {
            panic!()
        };
        assert_eq!(r.id, Id::Number(-5));
        assert!(matches!(r.method, Cow::Borrowed("m")));
        assert_eq!(
            r.params.map(|p| p.as_str().to_string()).as_deref(),
            Some(r#"{"a":1}"#)
        );
        let Ok(Message::Notification(n)) =
            single(r#"{"method":"x","jsonrpc":"2.0","params":null}"#)
        else {
            panic!()
        };
        assert_eq!(n.method, "x");
        assert!(n.params.is_none());
    }

    #[test]
    fn test_escaped_strings_decode() {
        let Ok(Message::Request(r)) = single(r#"{"jsonrpc":"2.0","id":"a\nb","method":"m"}"#)
        else {
            panic!()
        };
        assert_eq!(r.id.as_str(), Some("a\nb"));
        assert_eq!(r.method, "m");
    }

    #[test]
    fn test_invalid_ids() {
        for (id, answered) in [
            ("null", false),
            ("1.5", false),
            ("1e3", false),
            ("[1]", false),
            ("true", false),
            ("99999999999999999999", false),
        ] {
            let body = alloc::format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"m"}}"#);
            let invalid = invalid(&body).into_owned();
            assert!(!invalid.is_response());
            assert_eq!(invalid.id().is_some(), answered, "{id}");
        }
    }

    #[test]
    fn test_shape_errors_keep_valid_id() {
        for body in [
            r#"{"id":3,"method":"m"}"#,
            r#"{"jsonrpc":"1.0","id":3,"method":"m"}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":5}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"m","params":"x"}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"m","result":1}"#,
            r#"{"jsonrpc":"2.0","id":3,"method":"m","params":[],"params":{}}"#,
        ] {
            let invalid = invalid(body);
            assert_eq!(invalid.id(), Some(&Id::Number(3)), "{body}");
            assert_eq!(invalid.error().code(), ErrorCode::INVALID_REQUEST, "{body}");
        }
        // A repeated id cannot be trusted.
        assert!(
            invalid(r#"{"jsonrpc":"2.0","id":3,"id":4,"method":"m"}"#)
                .id()
                .is_none()
        );
    }

    #[test]
    fn test_responses() {
        let Ok(Message::Response(r)) = single(r#"{"jsonrpc":"2.0","id":2,"result":[1]}"#) else {
            panic!()
        };
        assert_eq!(
            r.result.map(|raw| raw.as_str().to_string()),
            Ok("[1]".to_string())
        );
        let Ok(Message::Response(r)) = single(
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"x","data":{"k":[1]}}}"#,
        ) else {
            panic!()
        };
        assert!(r.id.is_none());
        let error = r.result.err();
        assert_eq!(
            error.as_ref().map(ErrorObject::code),
            Some(ErrorCode::PARSE_ERROR)
        );
        assert_eq!(
            error.as_ref().and_then(|e| e.data()).map(Raw::as_str),
            Some(r#"{"k":[1]}"#)
        );
    }

    #[test]
    fn test_malformed_responses_are_response_like() {
        for body in [
            r#"{"jsonrpc":"2.0","id":2,"result":1,"error":{"code":1,"message":"m"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"error":{"code":1.5,"message":"m"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"error":{"message":"m"}}"#,
            r#"{"jsonrpc":"2.0","id":2,"error":"oops"}"#,
            r#"{"jsonrpc":"2.0","result":1}"#,
            r#"{"jsonrpc":"2.0","id":null,"result":1}"#,
        ] {
            let invalid = invalid(body);
            assert!(invalid.is_response(), "{body}");
            assert!(invalid.reply().is_none(), "{body}");
        }
    }

    #[test]
    fn test_not_messages() {
        for (body, code) in [
            ("", ErrorCode::PARSE_ERROR),
            ("{", ErrorCode::PARSE_ERROR),
            (r#"{"a":1} x"#, ErrorCode::PARSE_ERROR),
            ("\u{feff}{}", ErrorCode::PARSE_ERROR),
            ("1", ErrorCode::INVALID_REQUEST),
            ("{}", ErrorCode::INVALID_REQUEST),
            ("[]", ErrorCode::INVALID_REQUEST),
        ] {
            let invalid = invalid(body);
            assert_eq!(invalid.error().code(), code, "{body:?}");
            assert!(invalid.id().is_none());
        }
        let not_utf8 = decode(b"{\"a\":\"\xff\"}", &Limits::new());
        assert!(
            matches!(not_utf8, Incoming::Single(Err(i)) if i.error().code() == ErrorCode::PARSE_ERROR)
        );
    }

    #[test]
    fn test_batch_elements() {
        let body = br#"[1, [2, [3]], {"jsonrpc":"2.0","method":"n"}, {"foo":"boo"}]"#;
        let Incoming::Batch(entries) = decode(body, &Limits::new()) else {
            panic!()
        };
        assert_eq!(entries.len(), 4);
        assert!(entries[0].is_err() && entries[1].is_err() && entries[3].is_err());
        assert!(matches!(entries[2], Ok(Message::Notification(_))));
        // Invalid JSON anywhere in a batch is one parse error.
        assert_eq!(
            invalid(r#"[{"jsonrpc":"2.0","method":"n"}, {"#)
                .error()
                .code(),
            ErrorCode::PARSE_ERROR
        );
    }

    #[test]
    fn test_batch_limit() {
        let limits = Limits::new().with_max_batch(2);
        let body = br#"[{"jsonrpc":"2.0","method":"n"},{"jsonrpc":"2.0","method":"n"}]"#;
        assert!(matches!(decode(body, &limits), Incoming::Batch(e) if e.len() == 2));
        let body = br#"[{"jsonrpc":"2.0","method":"n"},{"jsonrpc":"2.0","method":"n"},1]"#;
        assert!(
            matches!(decode(body, &limits), Incoming::Single(Err(i)) if i.error().code() == ErrorCode::INVALID_REQUEST)
        );
    }

    #[test]
    fn test_budget_errors_salvage_the_id() {
        let limits = Limits::new().with_json(jsonx_lib::Limits::new().with_max_depth(2));
        let body = br#"{"jsonrpc":"2.0","id":"q","method":"m","params":[[[1]]]}"#;
        let Incoming::Single(Err(invalid)) = decode(body, &limits) else {
            panic!()
        };
        assert_eq!(invalid.id(), Some(&Id::from("q")));
        assert_eq!(invalid.error().code(), ErrorCode::INVALID_REQUEST);
        // Without a method first, nothing is salvaged.
        let body = br#"{"id":"q","params":[[[1]]],"method":"m"}"#;
        let Incoming::Single(Err(invalid)) = decode(body, &limits) else {
            panic!()
        };
        assert!(invalid.id().is_none());
    }

    #[test]
    fn test_write_round_trips() {
        let messages = [
            r#"{"jsonrpc":"2.0","id":"x\"y","method":"a/b","params":[1,{"k":null}]}"#,
            r#"{"jsonrpc":"2.0","method":"n"}"#,
            r#"{"jsonrpc":"2.0","id":1,"result":{"a":[]}}"#,
            r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"m","data":[true]}}"#,
        ];
        for text in messages {
            let Ok(message) = single(text) else {
                panic!("{text}")
            };
            assert_eq!(message.to_string(), text);
        }
    }
}
