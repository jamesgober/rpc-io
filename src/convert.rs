//! Typed values through jsonx-lib's [`FromJson`] and [`ToJson`]: the crate's
//! own typed API, with no third-party dependency.
//!
//! Decoding reads the validated [`Raw`] text the endpoint already holds, with
//! the endpoint's JSON budgets and duplicate-key policy, and borrows from it
//! where the target type does (`&str`, `Cow<str>`). Encoding writes straight
//! into a reused buffer: a typed result into one the endpoint keeps, typed
//! params and deferred answers into one per thread, so no `Value` tree and,
//! once the buffers have grown, no allocation stands between a value and the
//! wire.

use alloc::borrow::Cow;

use jsonx_lib::{
    DecodeError, DecodeErrorKind, FromJson, Kind, Number, RawValue, Reader, ToJson, ValueWriter,
    Written,
};

use crate::{
    error_object::{ErrorCode, ErrorObject},
    id::Id,
    raw::{Json, Raw},
};

impl Raw<'_> {
    /// Decodes the value into `T`, borrowing from this text where `T` does,
    /// with the default jsonx-lib budgets and duplicate keys refused.
    ///
    /// # Errors
    ///
    /// The `jsonx_lib::DecodeError` saying what did not fit, where (a byte
    /// offset and a JSON Pointer), and why.
    ///
    /// # Examples
    ///
    /// ```
    /// let raw = rpc_io::Raw::new(r#"["a", "b"]"#)?;
    /// let items: Vec<&str> = raw.decode()?; // borrowed from `raw`
    /// assert_eq!(items, ["a", "b"]);
    /// assert!(raw.decode::<Vec<u32>>().is_err());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn decode<'de, T: FromJson<'de>>(&'de self) -> Result<T, DecodeError> {
        jsonx_lib::from_str(self.as_str())
    }
}

impl Json<'static> {
    /// Encodes `value` as compact JSON, to send as params, a result, or
    /// error data. Infallible: a `ToJson` always writes one JSON value.
    ///
    /// This allocates the text. The typed methods that send a value
    /// (`Peer::notify_json`, `Responder::ok_json`, `Router::request_encoded`,
    /// ...) write into a reused buffer instead.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{ErrorObject, Json};
    ///
    /// let error = ErrorObject::invalid_params("bad line").with_data(Json::encode(&[3, 4]));
    /// assert_eq!(error.data().map(|d| d.as_str()), Some("[3,4]"));
    /// ```
    #[must_use]
    pub fn encode<T: ToJson + ?Sized>(value: &T) -> Json<'static> {
        Json::Raw(Raw::trusted_owned(jsonx_lib::to_string(value)))
    }
}

/// The JSON form: an integer, a string, a [`RawNumber`](Id::RawNumber)'s text,
/// or `null`.
impl ToJson for Id<'_> {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        match self {
            Id::Number(n) => out.i64(*n),
            Id::String(s) => out.str(s),
            Id::RawNumber(text) => match Number::from_text(text) {
                Some(number) => out.number(&number),
                // Not a JSON number: an id built by hand against its
                // contract. `null` keeps the output JSON.
                None => out.null(),
            },
            Id::Null => out.null(),
        }
    }
}

/// A string or an integer within `i64`, the ids LSP and MCP allow (and the
/// default policy of a decoded message); a string without escapes borrows
/// from the input.
impl<'de> FromJson<'de> for Id<'de> {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        const EXPECTED: &str = "a string or an integer id";
        let at = reader.offset();
        match reader.peek_kind()? {
            Some(Kind::String) => Cow::<'de, str>::from_json(reader).map(Id::String),
            Some(Kind::Number) => {
                let text = reader.read_raw()?.unwrap_or_default();
                Number::from_text(text)
                    .and_then(|n| n.as_i64())
                    .map(Id::Number)
                    .ok_or_else(|| {
                        let expected = "an integer id within i64";
                        DecodeError::new(DecodeErrorKind::OutOfRange { expected }, at)
                    })
            }
            Some(found) => Err(DecodeError::wrong_type(EXPECTED, found, at)),
            None => Err(DecodeError::invalid_value(EXPECTED, at)),
        }
    }
}

/// The integer.
impl ToJson for ErrorCode {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.i64(self.0)
    }
}

/// An integer within `i64`.
impl<'de> FromJson<'de> for ErrorCode {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        i64::from_json(reader).map(ErrorCode)
    }
}

/// The error object as a response carries it: `code`, `message`, and `data`
/// when there is some.
impl ToJson for ErrorObject {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.object(|o| {
            o.member("code", &self.code());
            o.member("message", self.message());
            if let Some(data) = self.data() {
                o.member("data", data);
            }
        })
    }
}

/// The text as it is. It is validated again on the way out: jsonx-lib's
/// writer writes raw text only as a `RawValue`, which it validates (its
/// output always parses back).
impl ToJson for Raw<'_> {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        match RawValue::new(self.as_str()) {
            Ok(raw) => out.raw(&raw),
            // A `Raw` holds one validated value, so this does not happen;
            // `null` keeps the output JSON if it ever did.
            Err(_) => out.null(),
        }
    }
}

/// One value, kept as its validated source text and borrowed from the
/// input: no decoding, no copy. Repeated keys inside it are kept as they are
/// (whoever parses it decides).
impl<'de> FromJson<'de> for Raw<'de> {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let at = reader.offset();
        match reader.read_raw()? {
            Some(text) => Ok(Raw::trusted(text)),
            None => Err(DecodeError::invalid_value("a JSON value", at)),
        }
    }
}

/// The value, or the text as it is.
impl ToJson for Json<'_> {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        match self {
            Json::Value(value) => out.value(value),
            Json::Raw(raw) => out.json(raw),
        }
    }
}

#[cfg(feature = "std")]
pub(crate) use handlers::with_buffer;

#[cfg(feature = "std")]
mod handlers {
    use std::cell::RefCell;

    use super::*;
    use crate::{
        error::{CallError, Error},
        error_object::clip_to,
        handles::{Call, Notifier, Peer, Responder},
        router::{Context, Params, Router},
    };

    /// A per-thread buffer is released after a value that grew it past
    /// this, as the endpoint's own buffers are.
    const BUFFER_KEEP: usize = 64 * 1024;

    std::thread_local! {
        /// Where typed params, deferred answers, and progress reports are
        /// encoded before they are copied into the endpoint's message
        /// buffer. Per thread, so encoding (user code, for a `ToJson`) runs
        /// outside the endpoint's lock and needs no lock of its own.
        static BUFFER: RefCell<String> = const { RefCell::new(String::new()) };
    }

    /// Runs `send` with the JSON `fill` writes, in this thread's reused
    /// buffer (or a fresh one, if `fill` or `send` encodes another value on
    /// this thread meanwhile). `fill` must write exactly one JSON value.
    pub(crate) fn with_buffer<R>(
        fill: impl FnOnce(&mut String),
        send: impl FnOnce(Raw<'_>) -> R,
    ) -> R {
        BUFFER.with(|cell| match cell.try_borrow_mut() {
            Ok(mut buffer) => {
                buffer.clear();
                fill(&mut buffer);
                let result = send(Raw::trusted(&buffer));
                if buffer.capacity() > BUFFER_KEEP {
                    *buffer = String::new();
                }
                result
            }
            Err(_) => {
                let mut fresh = String::new();
                fill(&mut fresh);
                send(Raw::trusted_owned(fresh))
            }
        })
    }

    /// The `INVALID_PARAMS` error for params that do not fit their type.
    /// The message names the path and offset, and may quote keys the peer
    /// chose: it is clipped, as every peer-controlled text the crate repeats
    /// is.
    fn mismatch(error: &DecodeError) -> ErrorObject {
        let text = error.to_string();
        ErrorObject::invalid_params(format!("invalid params: {}", clip_to(&text, 256)))
    }

    impl Params<'_> {
        /// Decodes the params into `T` (absent params read as `null`, so `()`
        /// and `Option<_>` accept them), with the endpoint's JSON budgets and
        /// duplicate keys refused. `T` may borrow from the message: `&str`
        /// for strings without escapes, `Cow<str>` for any.
        ///
        /// # Errors
        ///
        /// An `INVALID_PARAMS` [`ErrorObject`] saying what did not fit and
        /// where, ready to return with `?`.
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::jsonx::{DecodeError, FromJson, Reader};
        /// use rpc_io::{Endpoint, Router};
        ///
        /// struct Open<'a> { uri: &'a str, version: u32 }
        ///
        /// impl<'de> FromJson<'de> for Open<'de> {
        ///     fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        ///         let (mut uri, mut version) = (None, None);
        ///         let mut object = reader.read_object()?;
        ///         while let Some(key) = object.next_key()? {
        ///             match &*key {
        ///                 "uri" => object.field(&mut uri)?,
        ///                 "version" => object.field(&mut version)?,
        ///                 _ => {}
        ///             }
        ///         }
        ///         Ok(Open { uri: object.required(uri, "uri")?, version: object.required(version, "version")? })
        ///     }
        /// }
        ///
        /// let router = Router::new().request("open", |_: &mut (), params, _| {
        ///     let open: Open<'_> = params.decode()?; // `uri` borrows from the message
        ///     Ok(format!("{}@{}", open.uri, open.version))
        /// });
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// let mut endpoint = Endpoint::new(tx, router, ());
        /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"open","params":{"uri":"a.rs","version":2}}"#)?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":"a.rs@2"}"#));
        /// endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"open","params":{"uri":"a.rs"}}"#)?;
        /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32602") && m.contains("version")));
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        pub fn decode<'de, T: FromJson<'de>>(&'de self) -> Result<T, ErrorObject> {
            let text = self.text().unwrap_or("null");
            self.parser().decode(text).map_err(|e| mismatch(&e))
        }
    }

    /// Runs `send` with `value` encoded in this thread's reused buffer.
    fn with_encoded<T: ToJson + ?Sized, R>(value: &T, send: impl FnOnce(Raw<'_>) -> R) -> R {
        with_buffer(|out| jsonx_lib::write_value(out, value), send)
    }

    impl<S> Router<S> {
        /// Answers requests for `method` with a handler that returns `R`,
        /// encoded through jsonx-lib's [`ToJson`] straight into a buffer the
        /// endpoint reuses: no `Value` is built and, once the buffer has
        /// grown, nothing is allocated for the result.
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let router = Router::new().request_encoded("double", |_: &mut (), params, _| {
        ///     let p = params.parse()?;
        ///     let n = p[0].as_i64().unwrap_or(0);
        ///     Ok(vec![n, n * 2])
        /// });
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"double","params":[21]}"#)?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":[21,42]}"#));
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        #[must_use]
        pub fn request_encoded<R, F>(self, method: &str, mut handler: F) -> Self
        where
            R: ToJson,
            F: FnMut(&mut S, Params<'_>, &mut Context<'_>) -> Result<R, ErrorObject>
                + Send
                + 'static,
        {
            self.request_into(method, move |state, params, cx, out| {
                let result = handler(state, params, cx)?;
                out.clear();
                jsonx_lib::write_value(out, &result);
                Ok(Json::Raw(Raw::trusted(out)))
            })
        }
    }

    impl<S> Router<S> {
        /// Answers requests for `method` with a handler that takes its params
        /// as `P` and returns `R`, through jsonx-lib's traits.
        ///
        /// Params that do not decode into `P` are answered with
        /// `INVALID_PARAMS` without calling the handler; the result is
        /// written into the endpoint's reused buffer, as
        /// [`request_encoded`](Router::request_encoded) does.
        ///
        /// `P` is owned (`for<'de> FromJson<'de>`); for params that borrow
        /// from the message, take them as they come with `request_encoded`
        /// and decode them with [`Params::decode`].
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let router = Router::new().request_json("sum", |_: &mut (), numbers: Vec<i64>, _| {
        ///     Ok(numbers.iter().sum::<i64>())
        /// });
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// let mut endpoint = Endpoint::new(tx, router, ());
        /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"sum","params":[1,2,3]}"#)?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":6}"#));
        /// endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"sum","params":["x"]}"#)?;
        /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32602")));
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        #[must_use]
        pub fn request_json<P, R, F>(self, method: &str, mut handler: F) -> Self
        where
            P: for<'de> FromJson<'de>,
            R: ToJson,
            F: FnMut(&mut S, P, &mut Context<'_>) -> Result<R, ErrorObject> + Send + 'static,
        {
            self.request_encoded(method, move |state, params, cx| {
                let params = params.decode::<P>()?;
                handler(state, params, cx)
            })
        }

        /// Handles notifications for `method` with a handler that takes its
        /// params as `P`, through jsonx-lib's traits. Params that do not
        /// decode are reported as [`Event::InvalidParams`](crate::Event) and
        /// the handler is not called.
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let router = Router::new().notification_json("log", |seen: &mut Vec<String>, lines: Vec<String>, _| {
        ///     seen.extend(lines);
        /// });
        /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
        /// let mut endpoint = Endpoint::new(tx, router, Vec::new());
        /// endpoint.handle(br#"{"jsonrpc":"2.0","method":"log","params":["one","two"]}"#)?;
        /// endpoint.handle(br#"{"jsonrpc":"2.0","method":"log","params":[3]}"#)?; // reported, not handled
        /// assert_eq!(endpoint.state(), &["one", "two"]);
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        #[must_use]
        pub fn notification_json<P, F>(self, method: &str, mut handler: F) -> Self
        where
            P: for<'de> FromJson<'de>,
            F: FnMut(&mut S, P, &mut Context<'_>) + Send + 'static,
        {
            self.notification_with(method, move |state, params, cx| {
                let params = params.decode::<P>()?;
                handler(state, params, cx);
                Ok(())
            })
        }
    }

    impl Call {
        /// Blocks for the answer, as [`wait`](Call::wait), and decodes the
        /// result into `T` (owned: the answer does not outlive this call).
        ///
        /// # Errors
        ///
        /// As [`wait`](Call::wait); a result that does not decode into `T` is
        /// [`CallError::InvalidResponse`], with an `INVALID_PARAMS` error
        /// saying why.
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
        /// let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
        /// let call = endpoint.peer().request("pair", ())?;
        /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[1,"one"]}"#)?;
        /// assert_eq!(call.wait_json::<(u8, String)>(), Ok((1, "one".to_owned())));
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        pub fn wait_json<T: for<'de> FromJson<'de>>(self) -> Result<T, CallError> {
            let raw = self.wait()?;
            raw.decode::<T>()
                .map_err(|e| CallError::InvalidResponse(mismatch(&e)))
        }
    }

    impl Peer {
        /// Sends a notification whose params are `params`, encoded (they must
        /// encode to an array, an object, or `null` for none) in this
        /// thread's reused buffer.
        ///
        /// # Errors
        ///
        /// As [`Peer::notify`].
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
        /// endpoint.peer().notify_json("log", &["hi"])?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","method":"log","params":["hi"]}"#));
        /// assert!(endpoint.peer().notify_json("log", &5).is_err()); // not an array or object
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        pub fn notify_json<T: ToJson + ?Sized>(
            &self,
            method: &str,
            params: &T,
        ) -> Result<(), Error> {
            with_encoded(params, |raw| self.notify(method, raw))
        }

        /// Sends a request whose params are `params`, encoded as for
        /// [`notify_json`](Peer::notify_json); see [`Peer::request`].
        ///
        /// # Errors
        ///
        /// As [`Peer::request`].
        ///
        /// # Examples
        ///
        /// ```
        /// use rpc_io::{Endpoint, Router};
        ///
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
        /// let call = endpoint.peer().request_json("sum", &[1, 2, 3])?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"method":"sum","params":[1,2,3]}"#));
        /// call.detach();
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        pub fn request_json<T: ToJson + ?Sized>(
            &self,
            method: &str,
            params: &T,
        ) -> Result<Call, Error> {
            with_encoded(params, |raw| self.request(method, raw))
        }
    }

    impl Context<'_> {
        /// Sends a notification with typed params; see
        /// [`Peer::notify_json`].
        ///
        /// # Errors
        ///
        /// As [`Peer::notify`].
        ///
        /// # Examples
        ///
        /// ```
        /// let router: rpc_io::Router<()> = rpc_io::Router::new().notification("n", |_, _, cx| {
        ///     cx.notify_json("ack", &[true]).ok();
        /// });
        /// # let _ = router;
        /// ```
        pub fn notify_json<T: ToJson + ?Sized>(
            &self,
            method: &str,
            params: &T,
        ) -> Result<(), Error> {
            self.peer().notify_json(method, params)
        }

        /// Sends a request with typed params; see [`Peer::request_json`].
        ///
        /// # Errors
        ///
        /// As [`Peer::request`].
        ///
        /// # Examples
        ///
        /// ```
        /// let router = rpc_io::Router::new().notification("n", |calls: &mut Vec<rpc_io::Call>, _, cx| {
        ///     calls.extend(cx.request_json("ask", &["why"]).ok());
        /// });
        /// # let _ = router;
        /// ```
        pub fn request_json<T: ToJson + ?Sized>(
            &self,
            method: &str,
            params: &T,
        ) -> Result<Call, Error> {
            self.peer().request_json(method, params)
        }
    }

    impl Notifier {
        /// Sends a notification with typed params, encoded as for
        /// [`Peer::notify_json`], while the request is in flight.
        ///
        /// # Errors
        ///
        /// As [`Notifier::notify`].
        ///
        /// # Examples
        ///
        /// ```
        /// let router: rpc_io::Router<()> = rpc_io::Router::new().request("m", |_, _, cx| {
        ///     if let Some(n) = cx.notifier() {
        ///         n.notify_json("tick", &[1])?;
        ///     }
        ///     Ok(())
        /// });
        /// # let _ = router;
        /// ```
        pub fn notify_json<T: ToJson + ?Sized>(
            &self,
            method: &str,
            params: &T,
        ) -> Result<(), Error> {
            with_encoded(params, |raw| self.notify(method, raw))
        }
    }

    impl Responder {
        /// Answers with `value`, encoded in this thread's reused buffer.
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
        /// let router = Router::new().deferred("m", |_: &mut (), _, _, responder| {
        ///     responder.ok_json(&("a", 1)).ok();
        /// });
        /// let (tx, rx) = std::sync::mpsc::channel::<String>();
        /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
        /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":3,"result":["a",1]}"#));
        /// # Ok::<(), rpc_io::Error>(())
        /// ```
        pub fn ok_json<T: ToJson + ?Sized>(self, value: &T) -> Result<(), Error> {
            with_encoded(value, |raw| self.ok(raw))
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::string::ToString;

    use super::*;

    #[test]
    fn test_ids_and_codes_decode_strictly() {
        for id in [Id::from(-7), Id::from("x\"y")] {
            let text = jsonx_lib::to_string(&id);
            assert_eq!(jsonx_lib::from_str::<Id<'_>>(&text).ok(), Some(id));
        }
        for refused in ["18446744073709551615", "1.5", "null", "[]", "true"] {
            assert!(jsonx_lib::from_str::<Id<'_>>(refused).is_err(), "{refused}");
        }
        let text = alloc::string::String::from("\"plain\"");
        match jsonx_lib::from_str::<Id<'_>>(&text) {
            Ok(Id::String(Cow::Borrowed(s))) => assert_eq!(s, "plain"),
            other => panic!("{other:?}"),
        }
        assert_eq!(
            jsonx_lib::from_str::<ErrorCode>("-32601").ok(),
            Some(ErrorCode::METHOD_NOT_FOUND)
        );
    }

    #[test]
    fn test_error_objects_and_raw_values_encode_as_sent() {
        let error = ErrorObject::new(ErrorCode(7), "no").with_data(Json::encode(&[1]));
        assert_eq!(
            jsonx_lib::to_string(&error),
            r#"{"code":7,"message":"no","data":[1]}"#
        );
        let bare = ErrorObject::new(ErrorCode(7), "no");
        assert_eq!(jsonx_lib::to_string(&bare), r#"{"code":7,"message":"no"}"#);
        let raw = Raw::new(r#"{"a": [1, 2]}"#).unwrap_or(Raw::NULL);
        assert_eq!(jsonx_lib::to_string(&raw), r#"{"a": [1, 2]}"#);
        assert_eq!(jsonx_lib::to_string(&Json::from(raw)), r#"{"a": [1, 2]}"#);
        assert_eq!(
            jsonx_lib::to_string(&Json::from(alloc::vec![1, 2])),
            "[1,2]"
        );
        let raws: alloc::vec::Vec<Raw<'_>> =
            jsonx_lib::from_str(r#"[ {"k": 1} , "s" ]"#).unwrap_or_default();
        let texts: alloc::vec::Vec<&str> = raws.iter().map(Raw::as_str).collect();
        assert_eq!(texts, [r#"{"k": 1}"#, r#""s""#]);
        assert!(jsonx_lib::from_str::<Raw<'_>>("").is_err());
    }

    #[test]
    fn test_ids_and_codes_encode_as_json() {
        for id in [
            Id::from(-7),
            Id::from("x\"y"),
            Id::Null,
            Id::RawNumber("1e3".into()),
        ] {
            assert_eq!(jsonx_lib::to_string(&id), id.to_string());
        }
        assert_eq!(jsonx_lib::to_string(&Id::RawNumber("x".into())), "null");
        assert_eq!(jsonx_lib::to_string(&ErrorCode::METHOD_NOT_FOUND), "-32601");
        assert_eq!(Json::encode(&"q").to_string(), r#""q""#);
    }
}
