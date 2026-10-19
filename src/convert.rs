//! Typed values through jsonx-lib's [`ToJson`]: the crate's own typed API,
//! with no third-party dependency.
//!
//! Encoding writes straight into a reused buffer: a typed result into one the
//! endpoint keeps, typed params and deferred answers into one per thread, so
//! no `Value` tree and, once the buffers have grown, no allocation stands
//! between a value and the wire.

use jsonx_lib::{Number, ToJson, ValueWriter, Written};

use crate::{
    error_object::ErrorCode,
    id::Id,
    raw::{Json, Raw},
};

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

/// The integer.
impl ToJson for ErrorCode {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.i64(self.0)
    }
}

#[cfg(feature = "std")]
pub(crate) use handlers::with_buffer;

#[cfg(feature = "std")]
mod handlers {
    use std::cell::RefCell;

    use super::*;
    use crate::{
        error::Error,
        error_object::ErrorObject,
        handles::{Call, Peer, Responder},
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
