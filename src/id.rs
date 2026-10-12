//! [`Id`]: the id that pairs a request with its response.

use alloc::{borrow::Cow, string::String};
use core::fmt;

use crate::text;

/// A request id: an integer or a string.
///
/// JSON-RPC 2.0 also allows `null` and fractional numbers as ids, and
/// discourages both. LSP and MCP forbid them, and so does this crate: a request
/// whose id is `null`, a fraction, an integer outside `i64`, or any other kind
/// of value is refused as an invalid request and answered under a `null` id.
/// A `null` id remains legal in a [`Response`](crate::Response) to a message
/// whose id could not be read; there it is `None`.
///
/// String ids borrow from the message they were read from unless they contain
/// escape sequences. Ids compare by value, so `Ord` and `Hash` make them usable
/// as map keys.
///
/// # Examples
///
/// ```
/// use rpc_io::Id;
///
/// assert_eq!(Id::from(7).to_string(), "7");
/// assert_eq!(Id::from("abc").to_string(), r#""abc""#);
/// assert_eq!(Id::from(7).as_i64(), Some(7));
/// assert_eq!(Id::from("abc").as_str(), Some("abc"));
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Id<'a> {
    /// An integer id.
    Number(i64),
    /// A string id.
    String(Cow<'a, str>),
}

impl Id<'_> {
    /// The integer, if this is an integer id.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Id::from(-3).as_i64(), Some(-3));
    /// assert_eq!(rpc_io::Id::from("x").as_i64(), None);
    /// ```
    #[inline]
    #[must_use]
    pub const fn as_i64(&self) -> Option<i64> {
        match self {
            Id::Number(n) => Some(*n),
            Id::String(_) => None,
        }
    }

    /// The string, if this is a string id.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Id::from("req-1").as_str(), Some("req-1"));
    /// assert_eq!(rpc_io::Id::from(1).as_str(), None);
    /// ```
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Id::Number(_) => None,
            Id::String(s) => Some(s),
        }
    }

    /// Copies a borrowed string id so it can outlive the message it came from.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::Id;
    ///
    /// let text = String::from("job-9");
    /// let id: Id<'static> = Id::from(text.as_str()).into_owned();
    /// drop(text);
    /// assert_eq!(id.as_str(), Some("job-9"));
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Id<'static> {
        match self {
            Id::Number(n) => Id::Number(n),
            Id::String(s) => Id::String(Cow::Owned(s.into_owned())),
        }
    }

    /// Appends the id as JSON: digits, or a string literal.
    pub(crate) fn write(&self, out: &mut String) {
        match self {
            Id::Number(n) => text::push_i64(out, *n),
            Id::String(s) => jsonx_lib::write_string(out, s),
        }
    }
}

impl fmt::Display for Id<'_> {
    /// Writes the id as it appears in JSON.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = String::new();
        self.write(&mut out);
        f.write_str(&out)
    }
}

impl From<i64> for Id<'_> {
    #[inline]
    fn from(n: i64) -> Self {
        Id::Number(n)
    }
}

impl From<i32> for Id<'_> {
    #[inline]
    fn from(n: i32) -> Self {
        Id::Number(i64::from(n))
    }
}

impl From<u32> for Id<'_> {
    #[inline]
    fn from(n: u32) -> Self {
        Id::Number(i64::from(n))
    }
}

impl<'a> From<&'a str> for Id<'a> {
    #[inline]
    fn from(s: &'a str) -> Self {
        Id::String(Cow::Borrowed(s))
    }
}

impl From<String> for Id<'_> {
    #[inline]
    fn from(s: String) -> Self {
        Id::String(Cow::Owned(s))
    }
}

impl<'a> From<Cow<'a, str>> for Id<'a> {
    #[inline]
    fn from(s: Cow<'a, str>) -> Self {
        Id::String(s)
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::string::ToString;

    use super::*;

    #[test]
    fn test_display_writes_json() {
        assert_eq!(Id::from(i64::MIN).to_string(), "-9223372036854775808");
        assert_eq!(Id::from("a\"b").to_string(), r#""a\"b""#);
    }

    #[test]
    fn test_ids_order_numbers_before_strings() {
        assert!(Id::from(5) < Id::from("0"));
        assert!(Id::from(-1) < Id::from(0));
    }
}
