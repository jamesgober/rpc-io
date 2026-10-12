//! [`Raw`]: validated JSON kept as text, and [`Json`]: a value to send, built
//! or raw.

use alloc::{borrow::Cow, string::String};
use core::fmt;

use jsonx_lib::{Kind, Parser, Value};

/// One JSON value kept as its exact, validated source text.
///
/// This is how `params` and `result` travel through the crate without being
/// decoded: an incoming message is validated once and its `params` handed to
/// the handler as a slice of the message (`Raw<'a>` borrows), to be parsed
/// only if and when the handler wants it. In the other direction, a `Raw`
/// passes pre-serialized JSON through unchanged, so forwarding a value costs a
/// copy, not a parse and a re-serialization.
///
/// A `Raw` always holds exactly one complete JSON value with no surrounding
/// whitespace: the constructors validate, and the crate builds them only from
/// text it has validated.
///
/// # Examples
///
/// ```
/// use rpc_io::Raw;
///
/// let raw = Raw::new(r#" {"line": 3} "#)?;
/// assert_eq!(raw.as_str(), r#"{"line": 3}"#);
/// assert_eq!(raw.parse()?["line"], 3);
/// assert!(Raw::new("{").is_err());
/// # Ok::<(), jsonx_lib::Error>(())
/// ```
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Raw<'a>(Cow<'a, str>);

impl<'a> Raw<'a> {
    /// `null`.
    pub const NULL: Raw<'static> = Raw(Cow::Borrowed("null"));

    /// Validates `text` as one JSON value (the default `jsonx_lib` budgets)
    /// and keeps it, minus surrounding whitespace, as a borrowed slice.
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::Error` for text that is not exactly one JSON value.
    ///
    /// # Examples
    ///
    /// ```
    /// let raw = rpc_io::Raw::new("[1, 2]")?;
    /// assert_eq!(raw.as_str(), "[1, 2]");
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    pub fn new(text: &'a str) -> Result<Self, jsonx_lib::Error> {
        Self::new_with(text, &Parser::new())
    }

    /// As [`new`](Raw::new), with `parser`'s budgets.
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::Error` for text that is not exactly one JSON value
    /// within `parser`'s limits.
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonx_lib::{Limits, Parser};
    ///
    /// let shallow = Parser::new().limits(Limits::new().with_max_depth(1));
    /// assert!(rpc_io::Raw::new_with("[1]", &shallow).is_ok());
    /// assert!(rpc_io::Raw::new_with("[[1]]", &shallow).is_err());
    /// ```
    pub fn new_with(text: &'a str, parser: &Parser) -> Result<Self, jsonx_lib::Error> {
        let mut reader = parser.reader(text);
        match reader.read_raw()? {
            Some(value) => {
                reader.finish()?;
                Ok(Raw(Cow::Borrowed(value)))
            }
            // `read_raw` finds no value only in input that holds none, which
            // the parser refuses with the precise error.
            None => parser.parse(text).map(|_| Raw(Cow::Borrowed(text.trim()))),
        }
    }

    /// Validates an owned string as one JSON value, as [`new`](Raw::new).
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::Error` for text that is not exactly one JSON value.
    ///
    /// # Examples
    ///
    /// ```
    /// let raw = rpc_io::Raw::from_string(String::from("true"))?;
    /// assert_eq!(raw.as_str(), "true");
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    pub fn from_string(text: String) -> Result<Raw<'static>, jsonx_lib::Error> {
        let value = Raw::new(&text)?;
        if value.as_str().len() == text.len() {
            Ok(Raw(Cow::Owned(text)))
        } else {
            Ok(value.into_owned())
        }
    }

    /// Serializes `value` (compact JSON).
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonx_lib::json;
    ///
    /// let raw = rpc_io::Raw::from_value(&json!({ "ok": true }));
    /// assert_eq!(raw.as_str(), r#"{"ok":true}"#);
    /// ```
    #[must_use]
    pub fn from_value(value: &Value<'_>) -> Raw<'static> {
        Raw(Cow::Owned(jsonx_lib::to_string(value)))
    }

    /// Wraps text the crate has already validated as one JSON value.
    #[inline]
    pub(crate) const fn trusted(text: &'a str) -> Self {
        Raw(Cow::Borrowed(text))
    }

    /// Wraps an owned string the crate wrote itself as one JSON value.
    #[inline]
    pub(crate) const fn trusted_owned(text: String) -> Raw<'static> {
        Raw(Cow::Owned(text))
    }

    /// The JSON text.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Raw::NULL.as_str(), "null");
    /// ```
    #[inline]
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The kind of value, read from its first byte without parsing.
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonx_lib::Kind;
    ///
    /// assert_eq!(rpc_io::Raw::new("[]")?.kind(), Kind::Array);
    /// assert_eq!(rpc_io::Raw::new("-1.5")?.kind(), Kind::Number);
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    #[must_use]
    pub fn kind(&self) -> Kind {
        kind_of(self.as_str())
    }

    /// Parses the text into a [`Value`] that borrows from it, with the default
    /// `jsonx_lib` budgets and duplicate keys refused.
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::Error` for a value over the default budgets (the text
    /// itself is always valid JSON), or with a duplicate key.
    ///
    /// # Examples
    ///
    /// ```
    /// let raw = rpc_io::Raw::new(r#"{"a": [1, "two"]}"#)?;
    /// assert_eq!(raw.parse()?["a"][1], "two");
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    pub fn parse(&self) -> Result<Value<'_>, jsonx_lib::Error> {
        self.parse_with(&Parser::new())
    }

    /// Parses the text with `parser`'s budgets and duplicate-key policy.
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::Error` for a value over `parser`'s budgets, or that its
    /// duplicate-key policy refuses.
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonx_lib::{DuplicateKeys, Parser};
    ///
    /// let raw = rpc_io::Raw::new(r#"{"a": 1, "a": 2}"#)?;
    /// assert!(raw.parse().is_err());
    /// let last = Parser::new().duplicate_keys(DuplicateKeys::Last);
    /// assert_eq!(raw.parse_with(&last)?["a"], 2);
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    pub fn parse_with(&self, parser: &Parser) -> Result<Value<'_>, jsonx_lib::Error> {
        parser.parse(self.as_str())
    }

    /// Copies borrowed text so the value can outlive its message.
    ///
    /// # Examples
    ///
    /// ```
    /// let text = String::from("[1]");
    /// let raw: rpc_io::Raw<'static> = rpc_io::Raw::new(&text)?.into_owned();
    /// drop(text);
    /// assert_eq!(raw.as_str(), "[1]");
    /// # Ok::<(), jsonx_lib::Error>(())
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Raw<'static> {
        Raw(Cow::Owned(self.0.into_owned()))
    }

    /// A borrowed view of this value.
    #[inline]
    pub(crate) fn reborrow(&self) -> Raw<'_> {
        Raw(Cow::Borrowed(&self.0))
    }
}

/// The kind of the JSON value whose text starts `text`.
pub(crate) fn kind_of(text: &str) -> Kind {
    match text.as_bytes().first() {
        Some(b'{') => Kind::Object,
        Some(b'[') => Kind::Array,
        Some(b'"') => Kind::String,
        Some(b't' | b'f') => Kind::Bool,
        Some(b'n') => Kind::Null,
        _ => Kind::Number,
    }
}

impl fmt::Display for Raw<'_> {
    /// Writes the JSON text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Debug for Raw<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A JSON value to send: built as a [`Value`], or already serialized as a
/// [`Raw`].
///
/// Everything that sends JSON (params of requests and notifications, results,
/// error data) takes `impl Into<Json>`, so a `Value` from
/// [`json!`](jsonx_lib::json), anything that converts into a `Value` (numbers,
/// strings, booleans, `()` for `null`), or a `Raw` can be passed directly.
///
/// # Examples
///
/// ```
/// use jsonx_lib::json;
/// use rpc_io::{Json, Raw};
///
/// assert_eq!(Json::from(json!([1, 2])).to_string(), "[1,2]");
/// assert_eq!(Json::from(42).to_string(), "42");
/// assert_eq!(Json::from(()).to_string(), "null");
/// assert_eq!(Json::from(Raw::new(r#"{"a": 1}"#)?).to_string(), r#"{"a": 1}"#);
/// # Ok::<(), jsonx_lib::Error>(())
/// ```
#[derive(Clone, PartialEq)]
pub enum Json<'a> {
    /// A value, serialized when it is sent.
    Value(Value<'a>),
    /// Text sent as it is.
    Raw(Raw<'a>),
}

impl Json<'_> {
    /// The kind of value.
    ///
    /// # Examples
    ///
    /// ```
    /// use jsonx_lib::Kind;
    ///
    /// assert_eq!(rpc_io::Json::from(true).kind(), Kind::Bool);
    /// ```
    #[must_use]
    pub fn kind(&self) -> Kind {
        match self {
            Json::Value(value) => value.kind(),
            Json::Raw(raw) => raw.kind(),
        }
    }

    /// Copies everything borrowed.
    ///
    /// # Examples
    ///
    /// ```
    /// let text = String::from("a");
    /// let owned: rpc_io::Json<'static> = rpc_io::Json::from(text.as_str()).into_owned();
    /// drop(text);
    /// assert_eq!(owned.to_string(), r#""a""#);
    /// ```
    #[must_use]
    pub fn into_owned(self) -> Json<'static> {
        match self {
            Json::Value(value) => Json::Value(value.into_owned()),
            Json::Raw(raw) => Json::Raw(raw.into_owned()),
        }
    }

    /// Appends the value as compact JSON (raw text as it is).
    pub(crate) fn write(&self, out: &mut String) {
        match self {
            Json::Value(value) => jsonx_lib::write_value(out, value),
            Json::Raw(raw) => out.push_str(raw.as_str()),
        }
    }

    /// The value as a `Raw`, serializing a built value.
    pub(crate) fn into_raw(self) -> Raw<'static> {
        match self {
            Json::Value(value) => Raw::trusted_owned(jsonx_lib::to_string(&value)),
            Json::Raw(raw) => raw.into_owned(),
        }
    }
}

impl<'a, T: Into<Value<'a>>> From<T> for Json<'a> {
    #[inline]
    fn from(value: T) -> Self {
        Json::Value(value.into())
    }
}

impl<'a> From<Raw<'a>> for Json<'a> {
    #[inline]
    fn from(raw: Raw<'a>) -> Self {
        Json::Raw(raw)
    }
}

impl fmt::Display for Json<'_> {
    /// Writes compact JSON.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Json::Value(value) => fmt::Display::fmt(value, f),
            Json::Raw(raw) => f.write_str(raw.as_str()),
        }
    }
}

impl fmt::Debug for Json<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_new_trims_and_validates() {
        assert_eq!(Raw::new("\t null \n").map(|r| r.kind()), Ok(Kind::Null));
        assert!(Raw::new("").is_err());
        assert!(Raw::new("   ").is_err());
        assert!(Raw::new("1 2").is_err());
        assert!(Raw::new("[1,]").is_err());
    }

    #[test]
    fn test_from_string_keeps_or_trims() {
        let kept = Raw::from_string(String::from("[1]"));
        assert_eq!(kept.as_ref().map(Raw::as_str), Ok("[1]"));
        let trimmed = Raw::from_string(String::from(" [1] "));
        assert_eq!(trimmed.as_ref().map(Raw::as_str), Ok("[1]"));
    }

    #[test]
    fn test_kind_of_every_kind() {
        for (text, kind) in [
            ("{}", Kind::Object),
            ("[]", Kind::Array),
            ("\"\"", Kind::String),
            ("true", Kind::Bool),
            ("false", Kind::Bool),
            ("null", Kind::Null),
            ("-0", Kind::Number),
        ] {
            assert_eq!(kind_of(text), kind, "{text}");
        }
    }
}
