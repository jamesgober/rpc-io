//! Typed params and results through `serde` (the `serde` feature): the
//! ecosystem's protocol types (`lsp-types`, MCP schema crates, your own
//! `#[derive(Deserialize)]` structs) in and out of handlers and calls.
//!
//! Values are read from the validated [`Raw`] text the endpoint already holds
//! (so the JSON budgets of [`Limits`](crate::Limits) have been applied) and
//! written as compact JSON, with `serde_json`.

use std::fmt;

use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{self, DeserializeOwned, Visitor},
};

use crate::{
    error::{CallError, Error},
    error_object::{ErrorCode, ErrorObject},
    handles::{Call, Peer, Responder},
    id::Id,
    raw::{Json, Raw},
    router::{Context, Params, Router},
};

/// The `INVALID_PARAMS` error for a value that does not fit its type.
fn mismatch(error: &serde_json::Error) -> ErrorObject {
    ErrorObject::invalid_params(format!("invalid params: {error}"))
}

impl Params<'_> {
    /// Deserializes the params into `T` (absent params read as `null`, so
    /// `()` and `Option<_>` accept them).
    ///
    /// # Errors
    ///
    /// An `INVALID_PARAMS` [`ErrorObject`] saying what did not fit, ready to
    /// return with `?`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Position { line: u32, character: u32 }
    ///
    /// let router = Router::new().request("next", |_: &mut (), params, _| {
    ///     let p: Position = params.deserialize()?;
    ///     Ok(jsonx_lib::json!([p.line + 1, p.character]))
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, ());
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"next","params":{"line":3,"character":7}}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":[4,7]}"#));
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"next","params":{"line":-1}}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32602")));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn deserialize<'de, T: Deserialize<'de>>(&'de self) -> Result<T, ErrorObject> {
        let text = self.raw().map_or("null", Raw::as_str);
        serde_json::from_str(text).map_err(|e| mismatch(&e))
    }
}

impl Raw<'_> {
    /// Deserializes the value into `T`.
    ///
    /// # Errors
    ///
    /// An `INVALID_PARAMS` [`ErrorObject`] saying what did not fit.
    ///
    /// # Examples
    ///
    /// ```
    /// let raw = rpc_io::Raw::new(r#"{"tabSize": 4}"#)?;
    /// let config: std::collections::BTreeMap<String, u32> = raw.deserialize().map_err(|e| e.to_string())?;
    /// assert_eq!(config["tabSize"], 4);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn deserialize<'de, T: Deserialize<'de>>(&'de self) -> Result<T, ErrorObject> {
        serde_json::from_str(self.as_str()).map_err(|e| mismatch(&e))
    }
}

impl Json<'static> {
    /// Serializes `value` as compact JSON, to send as params, a result, or
    /// error data.
    ///
    /// # Errors
    ///
    /// An `INTERNAL_ERROR` [`ErrorObject`] if `value` cannot be written as
    /// JSON (a map with keys that are not strings, or a `Serialize` that
    /// fails), ready to return from a handler with `?`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::Json;
    ///
    /// #[derive(serde::Serialize)]
    /// struct Hover { contents: &'static str }
    ///
    /// let json = Json::serialize(&Hover { contents: "fn f()" })?;
    /// assert_eq!(json.to_string(), r#"{"contents":"fn f()"}"#);
    /// # Ok::<(), rpc_io::ErrorObject>(())
    /// ```
    pub fn serialize<T: Serialize + ?Sized>(value: &T) -> Result<Json<'static>, ErrorObject> {
        serialize(value)
            .map_err(|e| ErrorObject::internal_error(format!("the value is not JSON: {e}")))
    }
}

fn serialize<T: Serialize + ?Sized>(value: &T) -> Result<Json<'static>, serde_json::Error> {
    serde_json::to_string(value).map(|text| Json::Raw(Raw::trusted_owned(text)))
}

impl<S> Router<S> {
    /// Answers requests for `method` with a handler that takes its params as
    /// `P` and returns `R`, both through `serde`.
    ///
    /// Params that do not deserialize into `P` are answered with
    /// `INVALID_PARAMS` without calling the handler; a result that does not
    /// serialize is answered with `INTERNAL_ERROR`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    /// use serde::{Deserialize, Serialize};
    ///
    /// #[derive(Deserialize)]
    /// struct Add { a: i64, b: i64 }
    /// #[derive(Serialize)]
    /// struct Sum { sum: i64 }
    ///
    /// let router = Router::new().request_typed("add", |_: &mut (), p: Add, _| Ok(Sum { sum: p.a + p.b }));
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"add","params":{"a":2,"b":3}}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":{"sum":5}}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn request_typed<P, R, F>(self, method: &str, mut handler: F) -> Self
    where
        P: DeserializeOwned,
        R: Serialize,
        F: FnMut(&mut S, P, &mut Context<'_>) -> Result<R, ErrorObject> + Send + 'static,
    {
        self.request(method, move |state, params, cx| {
            let params = params.deserialize::<P>()?;
            let result = handler(state, params, cx)?;
            Json::serialize(&result)
        })
    }

    /// Handles notifications for `method` with a handler that takes its
    /// params as `P`, through `serde`. Params that do not deserialize are
    /// reported as [`Event::InvalidParams`](crate::Event::InvalidParams) and
    /// the handler is not called.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// #[derive(serde::Deserialize)]
    /// struct Opened { uri: String }
    ///
    /// let router = Router::new().notification_typed("didOpen", |open: &mut Vec<String>, p: Opened, _| {
    ///     open.push(p.uri);
    /// });
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, Vec::new());
    /// endpoint.handle(br#"{"jsonrpc":"2.0","method":"didOpen","params":{"uri":"file:///a"}}"#)?;
    /// assert_eq!(endpoint.state(), &["file:///a"]);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn notification_typed<P, F>(self, method: &str, mut handler: F) -> Self
    where
        P: DeserializeOwned,
        F: FnMut(&mut S, P, &mut Context<'_>) + Send + 'static,
    {
        self.notification_with(method, move |state, params, cx| {
            let params = params.deserialize::<P>()?;
            handler(state, params, cx);
            Ok(())
        })
    }
}

impl Peer {
    /// Sends a notification whose params are `params`, serialized (it must
    /// serialize to an array, an object, or `null` for none).
    ///
    /// # Errors
    ///
    /// [`Error::Serialize`] if `params` cannot be written as JSON; otherwise
    /// as [`Peer::notify`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// #[derive(serde::Serialize)]
    /// struct Log { message: &'static str }
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// endpoint.peer().notify_typed("log", &Log { message: "hi" })?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","method":"log","params":{"message":"hi"}}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn notify_typed<T: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &T,
    ) -> Result<(), Error> {
        let params = serialize(params).map_err(|e| Error::Serialize(e.to_string()))?;
        self.notify(method, params)
    }

    /// Sends a request whose params are `params`, serialized; see
    /// [`Peer::request`]. Await or wait for the answer as a type with
    /// [`Call::wait_typed`].
    ///
    /// # Errors
    ///
    /// [`Error::Serialize`] if `params` cannot be written as JSON; otherwise
    /// as [`Peer::request`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// let call = endpoint.peer().request_typed("sum", &[1, 2, 3])?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"method":"sum","params":[1,2,3]}"#));
    /// # let _ = call;
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn request_typed<T: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &T,
    ) -> Result<Call, Error> {
        let params = serialize(params).map_err(|e| Error::Serialize(e.to_string()))?;
        self.request(method, params)
    }
}

impl Call {
    /// Blocks for the answer, as [`wait`](Call::wait), and deserializes the
    /// result into `T`.
    ///
    /// # Errors
    ///
    /// As [`wait`](Call::wait); a result that does not deserialize into `T`
    /// is [`CallError::InvalidResponse`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// let call = endpoint.peer().request("sum", ())?;
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":6}"#)?;
    /// assert_eq!(call.wait_typed::<u64>(), Ok(6));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn wait_typed<T: DeserializeOwned>(self) -> Result<T, CallError> {
        let raw = self.wait()?;
        raw.deserialize().map_err(CallError::InvalidResponse)
    }
}

impl Responder {
    /// Answers with `value`, serialized.
    ///
    /// # Errors
    ///
    /// [`Error::Serialize`] if `value` cannot be written as JSON: the request
    /// is then answered with `INTERNAL_ERROR` instead, so it still gets its
    /// one response. Otherwise as [`respond`](Responder::respond).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let router = Router::new().deferred("m", |_: &mut (), _, _, responder| {
    ///     responder.ok_typed(&["a", "b"]).ok();
    /// });
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":3,"method":"m"}"#)?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":3,"result":["a","b"]}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn ok_typed<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
        match serialize(value) {
            Ok(json) => self.ok(json),
            Err(error) => {
                let refusal =
                    ErrorObject::internal_error("the result could not be written as JSON");
                let _answered = self.err(refusal);
                Err(Error::Serialize(error.to_string()))
            }
        }
    }
}

/// An integer, or a string.
impl Serialize for Id<'_> {
    fn serialize<Z: Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        match self {
            Id::Number(n) => serializer.serialize_i64(*n),
            Id::String(s) => serializer.serialize_str(s),
        }
    }
}

/// An integer within `i64`, or a string: the ids [`Id`] holds.
impl<'de> Deserialize<'de> for Id<'static> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct IdVisitor;

        impl Visitor<'_> for IdVisitor {
            type Value = Id<'static>;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("an integer or a string")
            }

            fn visit_i64<E: de::Error>(self, n: i64) -> Result<Self::Value, E> {
                Ok(Id::Number(n))
            }

            fn visit_u64<E: de::Error>(self, n: u64) -> Result<Self::Value, E> {
                i64::try_from(n)
                    .map(Id::Number)
                    .map_err(|_| E::custom("an integer id must fit in i64"))
            }

            fn visit_str<E: de::Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Id::from(s.to_owned()))
            }

            fn visit_string<E: de::Error>(self, s: String) -> Result<Self::Value, E> {
                Ok(Id::from(s))
            }
        }

        deserializer.deserialize_any(IdVisitor)
    }
}

/// The integer.
impl Serialize for ErrorCode {
    fn serialize<Z: Serializer>(&self, serializer: Z) -> Result<Z::Ok, Z::Error> {
        serializer.serialize_i64(self.0)
    }
}

/// An integer.
impl<'de> Deserialize<'de> for ErrorCode {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        i64::deserialize(deserializer).map(ErrorCode)
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    #[test]
    fn test_id_and_error_code_round_trip() {
        for id in [Id::from(-7), Id::from("x\"y")] {
            let text = serde_json::to_string(&id).unwrap_or_default();
            assert_eq!(text, id.to_string());
            assert_eq!(serde_json::from_str::<Id<'static>>(&text).ok(), Some(id));
        }
        assert!(serde_json::from_str::<Id<'static>>("18446744073709551615").is_err());
        assert!(serde_json::from_str::<Id<'static>>("1.5").is_err());
        assert_eq!(
            serde_json::from_str::<Id<'static>>("5").ok(),
            Some(Id::from(5))
        );
        let code = ErrorCode::METHOD_NOT_FOUND;
        assert_eq!(serde_json::to_string(&code).ok().as_deref(), Some("-32601"));
        assert_eq!(serde_json::from_str::<ErrorCode>("-32601").ok(), Some(code));
    }

    #[test]
    fn test_unserializable_values_are_errors() {
        let mut map = BTreeMap::new();
        let _none = map.insert(vec![1u8], 1u8); // non-string keys
        let error = Json::serialize(&map).err();
        assert_eq!(error.map(|e| e.code()), Some(ErrorCode::INTERNAL_ERROR));
    }
}
