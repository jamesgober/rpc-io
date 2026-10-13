//! [`ErrorObject`] and [`ErrorCode`]: the `error` member of a response.

use alloc::{borrow::Cow, format, string::String};
use core::fmt;

use crate::{
    raw::{Json, Raw},
    text,
};

/// A JSON-RPC error code.
///
/// The constants are the five codes JSON-RPC 2.0 defines, three the crate
/// itself answers with, and three more of LSP's for handlers to answer with.
/// Any integer is a valid code; protocols define their own, and a peer's code
/// is passed through as it was sent.
///
/// # Examples
///
/// ```
/// use rpc_io::ErrorCode;
///
/// assert_eq!(ErrorCode::METHOD_NOT_FOUND.0, -32601);
/// assert_eq!(ErrorCode::METHOD_NOT_FOUND.name(), Some("method not found"));
/// assert_eq!(ErrorCode(1001).name(), None);
/// assert_eq!(ErrorCode::INVALID_PARAMS.to_string(), "-32602 (invalid params)");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ErrorCode(pub i64);

impl ErrorCode {
    /// `-32700`: the message is not valid JSON.
    pub const PARSE_ERROR: ErrorCode = ErrorCode(-32700);
    /// `-32600`: the message is JSON but not a valid request object.
    pub const INVALID_REQUEST: ErrorCode = ErrorCode(-32600);
    /// `-32601`: no handler is registered for the method.
    pub const METHOD_NOT_FOUND: ErrorCode = ErrorCode(-32601);
    /// `-32602`: the params are not what the method takes.
    pub const INVALID_PARAMS: ErrorCode = ErrorCode(-32602);
    /// `-32603`: the server failed, for example a handler panicked.
    pub const INTERNAL_ERROR: ErrorCode = ErrorCode(-32603);
    /// `-32000`: too many requests are already being answered. The first code
    /// of the range JSON-RPC 2.0 reserves for implementation-defined server
    /// errors.
    pub const SERVER_OVERLOADED: ErrorCode = ErrorCode(-32000);
    /// `-32002`: a request arrived before the protocol's opening request
    /// succeeded (LSP's `ServerNotInitialized`); see
    /// [`Gate`](crate::Gate).
    pub const SERVER_NOT_INITIALIZED: ErrorCode = ErrorCode(-32002);
    /// `-32800`: the request was cancelled by its sender (LSP's
    /// `RequestCancelled`); see [`Cancellation`](crate::Cancellation).
    pub const REQUEST_CANCELLED: ErrorCode = ErrorCode(-32800);
    /// `-32801`: the document changed while the request was being answered,
    /// so the answer would be stale (LSP's `ContentModified`).
    pub const CONTENT_MODIFIED: ErrorCode = ErrorCode(-32801);
    /// `-32802`: the server cancelled the request itself (LSP's
    /// `ServerCancelled`, for requests that declare it retriable).
    pub const SERVER_CANCELLED: ErrorCode = ErrorCode(-32802);
    /// `-32803`: the request was valid but failed (LSP's `RequestFailed`).
    pub const REQUEST_FAILED: ErrorCode = ErrorCode(-32803);

    /// A short name for the codes this crate defines, `None` for others.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::ErrorCode::PARSE_ERROR.name(), Some("parse error"));
    /// ```
    #[must_use]
    pub const fn name(self) -> Option<&'static str> {
        Some(match self.0 {
            -32700 => "parse error",
            -32600 => "invalid request",
            -32601 => "method not found",
            -32602 => "invalid params",
            -32603 => "internal error",
            -32000 => "server overloaded",
            -32002 => "server not initialized",
            -32800 => "request cancelled",
            -32801 => "content modified",
            -32802 => "server cancelled",
            -32803 => "request failed",
            _ => return None,
        })
    }

    /// Whether the code lies in `-32768..=-32000`, the range JSON-RPC 2.0
    /// reserves for predefined and implementation-defined errors.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::ErrorCode;
    ///
    /// assert!(ErrorCode::INTERNAL_ERROR.is_reserved());
    /// assert!(!ErrorCode(1001).is_reserved());
    /// ```
    #[must_use]
    pub const fn is_reserved(self) -> bool {
        self.0 >= -32768 && self.0 <= -32000
    }
}

impl fmt::Display for ErrorCode {
    /// The number, with the name when the code has one.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.name() {
            Some(name) => write!(f, "{} ({name})", self.0),
            None => write!(f, "{}", self.0),
        }
    }
}

/// The `error` member of a response: a code, a message, and optional data.
///
/// Handlers return one to fail a request; the crate builds one for every
/// protocol error it answers itself; and a peer's error response arrives as
/// one. The message is kept owned (it is short, and an error usually outlives
/// the message it was read from); `data` is any JSON value, kept as text.
///
/// # Examples
///
/// ```
/// use jsonx_lib::json;
/// use rpc_io::{ErrorCode, ErrorObject};
///
/// let error = ErrorObject::new(ErrorCode(1001), "stale version")
///     .with_data(json!({ "expected": 4 }));
/// assert_eq!(error.code(), ErrorCode(1001));
/// assert_eq!(error.message(), "stale version");
/// assert_eq!(error.data().map(|d| d.as_str()), Some(r#"{"expected":4}"#));
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorObject {
    code: ErrorCode,
    message: Cow<'static, str>,
    data: Option<Raw<'static>>,
}

impl ErrorObject {
    /// An error with `code` and `message` and no data.
    ///
    /// # Examples
    ///
    /// ```
    /// let e = rpc_io::ErrorObject::new(rpc_io::ErrorCode(7), "nope");
    /// assert_eq!(e.to_string(), "nope (code 7)");
    /// ```
    #[must_use]
    pub fn new(code: ErrorCode, message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    /// Attaches `data`, any JSON value, replacing data set before.
    ///
    /// # Examples
    ///
    /// ```
    /// let e = rpc_io::ErrorObject::invalid_params("bad line").with_data(12);
    /// assert_eq!(e.data().map(|d| d.as_str()), Some("12"));
    /// ```
    #[must_use]
    pub fn with_data<'a>(mut self, data: impl Into<Json<'a>>) -> Self {
        self.data = Some(data.into().into_raw());
        self
    }

    /// `-32700` parse error with `message`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, ErrorObject};
    ///
    /// assert_eq!(ErrorObject::parse_error("bad").code(), ErrorCode::PARSE_ERROR);
    /// ```
    #[must_use]
    pub fn parse_error(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorCode::PARSE_ERROR, message)
    }

    /// `-32600` invalid request with `message`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, ErrorObject};
    ///
    /// assert_eq!(ErrorObject::invalid_request("bad").code(), ErrorCode::INVALID_REQUEST);
    /// ```
    #[must_use]
    pub fn invalid_request(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorCode::INVALID_REQUEST, message)
    }

    /// `-32601` method not found, naming `method`.
    ///
    /// # Examples
    ///
    /// ```
    /// let e = rpc_io::ErrorObject::method_not_found("frob");
    /// assert_eq!(e.message(), "method not found: `frob`");
    /// ```
    #[must_use]
    pub fn method_not_found(method: &str) -> Self {
        Self::new(
            ErrorCode::METHOD_NOT_FOUND,
            format!("method not found: `{}`", clip(method)),
        )
    }

    /// `-32602` invalid params with `message`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, ErrorObject};
    ///
    /// let e = ErrorObject::invalid_params("`line` must be an integer");
    /// assert_eq!(e.code(), ErrorCode::INVALID_PARAMS);
    /// ```
    #[must_use]
    pub fn invalid_params(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorCode::INVALID_PARAMS, message)
    }

    /// `-32603` internal error with `message`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorCode, ErrorObject};
    ///
    /// assert_eq!(ErrorObject::internal_error("x").code(), ErrorCode::INTERNAL_ERROR);
    /// ```
    #[must_use]
    pub fn internal_error(message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(ErrorCode::INTERNAL_ERROR, message)
    }

    /// The code.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::ErrorObject::new(rpc_io::ErrorCode(3), "m").code().0, 3);
    /// ```
    #[inline]
    #[must_use]
    pub const fn code(&self) -> ErrorCode {
        self.code
    }

    /// The message.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::ErrorObject::internal_error("boom").message(), "boom");
    /// ```
    #[inline]
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// The data, if any, as JSON text.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::ErrorObject::internal_error("boom").data().is_none());
    /// ```
    #[inline]
    #[must_use]
    pub fn data(&self) -> Option<&Raw<'static>> {
        self.data.as_ref()
    }

    /// Builds an error from fields read off the wire.
    pub(crate) fn from_parts(code: i64, message: String, data: Option<Raw<'static>>) -> Self {
        Self {
            code: ErrorCode(code),
            message: Cow::Owned(message),
            data,
        }
    }

    /// Appends the error object as JSON.
    pub(crate) fn write(&self, out: &mut String) {
        out.push_str("{\"code\":");
        text::push_i64(out, self.code.0);
        out.push_str(",\"message\":");
        jsonx_lib::write_string(out, &self.message);
        if let Some(data) = &self.data {
            out.push_str(",\"data\":");
            out.push_str(data.as_str());
        }
        out.push('}');
    }
}

impl fmt::Display for ErrorObject {
    /// `"<message> (code <code>)"`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (code {})", self.message, self.code.0)
    }
}

impl core::error::Error for ErrorObject {}

impl From<jsonx_lib::Error> for ErrorObject {
    /// An `-32602` invalid-params error describing why the params did not
    /// parse, so a handler can apply `?` to `params.parse()`.
    fn from(error: jsonx_lib::Error) -> Self {
        Self::invalid_params(format!("invalid params: {error}"))
    }
}

/// At most the first 128 bytes of `text` (on a character boundary), with an
/// ellipsis when cut, so peer-controlled text quoted in a message stays short.
pub(crate) fn clip(text: &str) -> Cow<'_, str> {
    clip_to(text, 128)
}

/// At most the first `max` bytes of `text` (on a character boundary), with an
/// ellipsis when cut.
pub(crate) fn clip_to(text: &str, max: usize) -> Cow<'_, str> {
    if text.len() <= max {
        return Cow::Borrowed(text);
    }
    let mut end = max;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!("{}\u{2026}", &text[..end]))
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::string::ToString;

    use super::*;

    #[test]
    fn test_write_with_and_without_data() {
        let mut out = String::new();
        ErrorObject::new(ErrorCode(-1), "a\"b").write(&mut out);
        assert_eq!(out, r#"{"code":-1,"message":"a\"b"}"#);
        out.clear();
        ErrorObject::internal_error("x")
            .with_data(jsonx_lib::json!([1]))
            .write(&mut out);
        assert_eq!(out, r#"{"code":-32603,"message":"x","data":[1]}"#);
    }

    #[test]
    fn test_clip_respects_char_boundaries() {
        let long = "é".repeat(100);
        let clipped = clip(&long);
        assert!(clipped.len() <= 128 + '\u{2026}'.len_utf8());
        assert!(clipped.ends_with('\u{2026}'));
        assert_eq!(clip("short"), "short");
    }

    #[test]
    fn test_from_jsonx_error_is_invalid_params() {
        let Err(e) = jsonx_lib::parse("[") else {
            panic!("`[` must not parse")
        };
        let error = ErrorObject::from(e);
        assert_eq!(error.code(), ErrorCode::INVALID_PARAMS);
        assert!(error.message().starts_with("invalid params: "));
        assert_eq!(
            error.to_string(),
            alloc::format!("{} (code -32602)", error.message())
        );
    }
}
