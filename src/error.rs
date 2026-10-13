//! [`Error`] (the connection) and [`CallError`] (one outgoing request).

use std::{fmt, io};

use jsonx_lib::Kind;

use crate::{error_object::ErrorObject, framing::FrameError};

/// Why the connection, or an operation on it, failed.
///
/// # Examples
///
/// ```
/// use rpc_io::{Error, FrameError, Framing, Router};
///
/// // A broken header section ends the session: the stream has no reliable
/// // message boundary left.
/// let input: &[u8] = b"Content-Length: x\r\n\r\n";
/// let result = rpc_io::serve(input, std::io::sink(), Framing::ContentLength, Router::new());
/// assert!(matches!(result, Err(Error::Frame(FrameError::InvalidContentLength))));
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Reading or writing the transport failed. The connection is closed.
    Io(io::Error),
    /// The input stream broke its framing in a way that leaves no reliable
    /// message boundary (see [`FrameError::is_recoverable`]). The connection is
    /// closed.
    Frame(FrameError),
    /// The connection is closed: the endpoint stopped, its input ended, or its
    /// output failed earlier. Nothing more can be sent.
    Closed,
    /// Outgoing `params` must be an array, an object, or `null` (sent as no
    /// params); this was another kind of value. Nothing was sent.
    InvalidParams(Kind),
    /// The request was already answered (the peer cancelled it, or the
    /// connection answered it for a panicking handler). This reply was not
    /// sent.
    AlreadyAnswered,
    /// The [`Gate`](crate::Gate) is shut and the messages held behind it
    /// already reach [`Config::max_held_bytes`](crate::Config). This one was
    /// not sent or held; the connection is still open.
    Backlog,
    /// A value could not be written as JSON (the `serde` feature's typed
    /// sends: a map whose keys are not strings, or a `Serialize` that
    /// failed); this says why. Nothing was sent.
    Serialize(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "the transport failed: {e}"),
            Error::Frame(e) => write!(f, "the input lost its framing: {e}"),
            Error::Closed => f.write_str("the connection is closed"),
            Error::InvalidParams(kind) => write!(
                f,
                "params must be an array, an object, or null, not {} {kind}",
                if matches!(kind, Kind::Array | Kind::Object) {
                    "an"
                } else {
                    "a"
                }
            ),
            Error::AlreadyAnswered => f.write_str("the request was already answered"),
            Error::Backlog => f.write_str(
                "too much is already held until the opening request succeeds; nothing was sent",
            ),
            Error::Serialize(why) => write!(f, "the value is not JSON: {why}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Frame(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<FrameError> for Error {
    fn from(e: FrameError) -> Self {
        Error::Frame(e)
    }
}

impl From<Error> for ErrorObject {
    /// An `-32603` internal error saying what failed, so a handler can apply
    /// `?` to a send through its [`Context`](crate::Context).
    ///
    /// An [`Error::Io`] says only that the transport failed: the operating
    /// system's text (which can name paths and hosts) stays on this side. Log
    /// the [`Error`] itself for the detail.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Error, ErrorCode, ErrorObject};
    ///
    /// let e = ErrorObject::from(Error::Closed);
    /// assert_eq!(e.code(), ErrorCode::INTERNAL_ERROR);
    /// assert_eq!(e.message(), "the connection is closed");
    ///
    /// let io = std::io::Error::other("C:\\secret\\path is locked");
    /// assert_eq!(ErrorObject::from(Error::Io(io)).message(), "the transport failed");
    /// ```
    fn from(error: Error) -> Self {
        match error {
            Error::Io(_) => ErrorObject::internal_error("the transport failed"),
            other => ErrorObject::internal_error(other.to_string()),
        }
    }
}

/// Why an outgoing request produced no result.
///
/// Every request ends in exactly one outcome: its result, or one of these.
///
/// # Examples
///
/// ```
/// use rpc_io::{CallError, ErrorObject};
///
/// let e = CallError::Rejected(ErrorObject::method_not_found("x"));
/// assert_eq!(e.to_string(), "the peer answered with an error: method not found: `x` (code -32601)");
/// assert_eq!(CallError::TimedOut.to_string(), "the request timed out");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CallError {
    /// The peer answered with this error.
    Rejected(ErrorObject),
    /// The peer's answer was not a valid response; this describes how.
    InvalidResponse(ErrorObject),
    /// The deadline passed without an answer. The peer was told to cancel.
    TimedOut,
    /// The request was cancelled (by [`Call::cancel`](crate::Call::cancel), a
    /// [`Canceller`](crate::Canceller), or because the gate's opening request
    /// failed before it was sent).
    Cancelled,
    /// More requests were outstanding than [`Config::max_pending`](crate::Config)
    /// allows and this was the oldest. The peer was told to cancel.
    Evicted,
    /// The connection closed before an answer arrived.
    Closed,
    /// Waiting here would block the thread that reads the answer: a handler
    /// cannot wait for a request it sent. Use [`Call::try_wait`](crate::Call::try_wait)
    /// later, or wait on another thread.
    WouldDeadlock,
}

impl fmt::Display for CallError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CallError::Rejected(e) => write!(f, "the peer answered with an error: {e}"),
            CallError::InvalidResponse(e) => write!(f, "the peer's answer was invalid: {e}"),
            CallError::TimedOut => f.write_str("the request timed out"),
            CallError::Cancelled => f.write_str("the request was cancelled"),
            CallError::Evicted => {
                f.write_str("the request was dropped: too many requests were outstanding")
            }
            CallError::Closed => f.write_str("the connection closed before an answer arrived"),
            CallError::WouldDeadlock => {
                f.write_str("waiting would block the endpoint's own thread, which reads the answer")
            }
        }
    }
}

impl std::error::Error for CallError {}
