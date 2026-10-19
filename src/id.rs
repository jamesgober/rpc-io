//! [`Id`]: the id that pairs a request with its response.

use alloc::{borrow::Cow, string::String};
use core::fmt;

use crate::text;

/// A request id: an integer or a string, and with lenient ids, `null` or any
/// other number.
///
/// JSON-RPC 2.0 also allows `null` and fractional numbers as ids, and
/// discourages both. LSP and MCP forbid them, and by default so does this
/// crate: a request whose id is `null`, a fraction, an integer outside `i64`,
/// or any other kind of value is refused as an invalid request and answered
/// under a `null` id. With [`Limits::lenient_ids`](crate::Limits) (for
/// generic JSON-RPC services) such a request is accepted instead, with the id
/// kept as [`Id::Null`] or [`Id::RawNumber`] and echoed back as it was sent.
/// A `null` id remains legal in a [`Response`](crate::Response) to a message
/// whose id could not be read; there it is `None`.
///
/// String ids borrow from the message they were read from unless they contain
/// escape sequences. Ids compare by value (a [`RawNumber`](Id::RawNumber) by
/// its text, so `1.0` and `1.00` are different ids), so `Ord` and `Hash` make
/// them usable as map keys.
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
/// assert_eq!(Id::Null.to_string(), "null");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Id<'a> {
    /// An integer id.
    Number(i64),
    /// A string id.
    String(Cow<'a, str>),
    /// Any other JSON number: a fraction, an exponent, or an integer outside
    /// `i64`, kept as its exact text. Read only with
    /// [`Limits::lenient_ids`](crate::Limits); built by hand, the text must
    /// be a valid JSON number (see [`Id::raw_number`]).
    RawNumber(Cow<'a, str>),
    /// A `null` request id. Read only with
    /// [`Limits::lenient_ids`](crate::Limits). Two requests with `null` ids
    /// cannot both be in flight: the second is refused as a duplicate.
    Null,
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
            Id::String(_) | Id::RawNumber(_) | Id::Null => None,
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
            Id::String(s) => Some(s),
            Id::Number(_) | Id::RawNumber(_) | Id::Null => None,
        }
    }

    /// A [`RawNumber`](Id::RawNumber) id from `text`, if `text` is exactly
    /// one JSON number. An integer within `i64` gives an
    /// [`Number`](Id::Number) id instead, as a decoded message would.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::Id;
    ///
    /// assert_eq!(Id::raw_number("1.5"), Some(Id::RawNumber("1.5".into())));
    /// assert_eq!(Id::raw_number("12"), Some(Id::Number(12)));
    /// assert_eq!(Id::raw_number("1."), None);
    /// ```
    #[must_use]
    pub fn raw_number(text: &str) -> Option<Id<'_>> {
        let number = jsonx_lib::Number::from_text(text)?;
        Some(match number.as_i64() {
            Some(n) => Id::Number(n),
            None => Id::RawNumber(Cow::Borrowed(text)),
        })
    }

    /// Whether this is a `null` id.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::Id::Null.is_null());
    /// assert!(!rpc_io::Id::from(0).is_null());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Id::Null)
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
            Id::RawNumber(s) => Id::RawNumber(Cow::Owned(s.into_owned())),
            Id::Null => Id::Null,
        }
    }

    /// Appends the id as JSON: digits, a string literal, a number's text, or
    /// `null`.
    pub(crate) fn write(&self, out: &mut String) {
        match self {
            Id::Number(n) => text::push_i64(out, *n),
            Id::String(s) => jsonx_lib::write_string(out, s),
            Id::RawNumber(s) => out.push_str(s),
            Id::Null => out.push_str("null"),
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
    fn test_lenient_ids_write_as_sent() {
        assert_eq!(
            Id::raw_number("-1.5e300")
                .map(|id| id.to_string())
                .as_deref(),
            Some("-1.5e300")
        );
        assert_eq!(
            Id::raw_number("18446744073709551616").map(|id| id.as_i64()),
            Some(None)
        );
        assert_eq!(Id::raw_number("abc"), None);
        assert_eq!(Id::raw_number(" 1"), None);
        assert_eq!(Id::Null.as_str(), None);
        assert_eq!(Id::Null.into_owned(), Id::Null);
    }

    #[test]
    fn test_ids_order_numbers_before_strings() {
        assert!(Id::from(5) < Id::from("0"));
        assert!(Id::from(-1) < Id::from(0));
    }
}
