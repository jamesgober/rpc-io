//! Properties: arbitrary input never panics; every message shape survives
//! encoding and decoding (through either framing); envelope classification
//! matches a simple reference built on a full parse; batch responses carry
//! exactly the ids of the batch's requests.
#![cfg(feature = "std")]

mod common;

use std::{borrow::Cow, collections::BTreeMap};

use common::{quiet_deliberate_panics, sent};
use jsonx_lib::{DuplicateKeys, Object, Parser, Value};
use proptest::prelude::*;
use rpc_io::{
    Config, Endpoint, ErrorCode, ErrorObject, FrameError, Framing, Id, Incoming, Limits, Message,
    Notification, Raw, Request, Responder, Response, Router, decode, write_batch,
};

// --- Generators --------------------------------------------------------------

fn text() -> impl Strategy<Value = String> {
    prop_oneof![
        3 => "[a-z/$._]{0,10}",
        1 => proptest::collection::vec(any::<char>(), 0..8).prop_map(String::from_iter),
        1 => Just("q\"uo\\te\n\t\u{0}\u{1f}\u{2028}é😀".to_owned()),
    ]
}

fn value() -> impl Strategy<Value = Value<'static>> {
    let leaf = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        prop_oneof![Just(0.5f64), Just(-1.25e10), Just(3.0e-7)].prop_map(Value::from),
        text().prop_map(Value::from),
    ];
    leaf.prop_recursive(4, 24, 4, |inner| {
        prop_oneof![
            proptest::collection::vec(inner.clone(), 0..4).prop_map(Value::from),
            proptest::collection::vec((text(), inner), 0..4).prop_map(|members| {
                let members: Vec<(Cow<'static, str>, Value<'static>)> = members
                    .into_iter()
                    .map(|(k, v)| (Cow::Owned(k), v))
                    .collect();
                Value::from(Object::from(members))
            }),
        ]
    })
}

fn raw() -> impl Strategy<Value = Raw<'static>> {
    value().prop_map(|v| Raw::from_value(&v))
}

fn structured() -> impl Strategy<Value = Raw<'static>> {
    value().prop_map(|v| match v {
        v @ (Value::Array(_) | Value::Object(_)) => Raw::from_value(&v),
        other => Raw::from_value(&Value::from(vec![other])),
    })
}

fn id() -> impl Strategy<Value = Id<'static>> {
    prop_oneof![any::<i64>().prop_map(Id::Number), text().prop_map(Id::from)]
}

fn error_object() -> impl Strategy<Value = ErrorObject> {
    (any::<i64>(), text(), proptest::option::of(raw())).prop_map(|(code, message, data)| {
        let error = ErrorObject::new(ErrorCode(code), message);
        match data {
            Some(data) => error.with_data(data),
            None => error,
        }
    })
}

fn message() -> impl Strategy<Value = Message<'static>> {
    prop_oneof![
        (id(), text(), proptest::option::of(structured())).prop_map(|(id, method, params)| {
            Message::Request(Request {
                id,
                method: method.into(),
                params,
            })
        }),
        (text(), proptest::option::of(structured())).prop_map(|(method, params)| {
            Message::Notification(Notification {
                method: method.into(),
                params,
            })
        }),
        (id(), raw()).prop_map(|(id, result)| Message::Response(Response {
            id: Some(id),
            result: Ok(result)
        })),
        (proptest::option::of(id()), error_object()).prop_map(|(id, error)| Message::Response(
            Response {
                id,
                result: Err(error)
            }
        )),
    ]
}

/// Frames `bodies` and cuts them back out, feeding `chunk` bytes at a time.
fn reframe(framing: Framing, bodies: &[String], chunk: usize) -> Vec<Vec<u8>> {
    let mut wire = Vec::new();
    for body in bodies {
        framing.encode(body.as_bytes(), &mut wire);
    }
    let mut decoder = framing.decoder(&Limits::new());
    let mut out = Vec::new();
    let mut rest = wire.as_slice();
    loop {
        while let Ok(Some(frame)) = decoder.next_frame() {
            out.push(frame.to_vec());
        }
        if rest.is_empty() {
            break;
        }
        let n = decoder.feed(&rest[..rest.len().min(chunk)]);
        rest = &rest[n..];
    }
    while let Ok(Some(frame)) = decoder.finish() {
        out.push(frame.to_vec());
    }
    out
}

// --- Round trips -------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_message_round_trips(message in message()) {
        let text = message.to_string();
        let decoded = decode(text.as_bytes(), &Limits::new());
        prop_assert_eq!(decoded, Incoming::Single(Ok(message)));
    }

    #[test]
    fn prop_messages_round_trip_through_both_framings(
        messages in proptest::collection::vec(message(), 1..6),
        chunk in 1usize..200,
        newline in any::<bool>(),
    ) {
        let framing = if newline { Framing::Newline } else { Framing::ContentLength };
        let bodies: Vec<String> = messages.iter().map(ToString::to_string).collect();
        let frames = reframe(framing, &bodies, chunk);
        prop_assert_eq!(frames.len(), messages.len());
        for (frame, message) in frames.iter().zip(&messages) {
            prop_assert_eq!(decode(frame, &Limits::new()), Incoming::Single(Ok(message.clone())));
        }
    }

    #[test]
    fn prop_batch_round_trips(messages in proptest::collection::vec(message(), 1..8)) {
        let mut text = String::new();
        write_batch(&mut text, &messages);
        let decoded = decode(text.as_bytes(), &Limits::new());
        let expected: Vec<_> = messages.into_iter().map(Ok).collect();
        prop_assert_eq!(decoded, Incoming::Batch(expected));
    }

    #[test]
    fn prop_any_body_round_trips_through_content_length(
        bodies in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..64), 0..6),
        chunk in 1usize..40,
    ) {
        let mut wire = Vec::new();
        for body in &bodies {
            Framing::ContentLength.encode(body, &mut wire);
        }
        let mut decoder = Framing::ContentLength.decoder(&Limits::new());
        let mut out = Vec::new();
        for piece in wire.chunks(chunk) {
            let mut piece = piece;
            while !piece.is_empty() {
                let n = decoder.feed(piece);
                piece = &piece[n..];
                while let Ok(Some(frame)) = decoder.next_frame() {
                    out.push(frame.to_vec());
                }
            }
        }
        prop_assert_eq!(decoder.finish(), Ok(None));
        prop_assert_eq!(out, bodies);
    }
}

// --- Arbitrary input -----------------------------------------------------------

/// Bytes biased toward JSON-RPC syntax, so the envelope logic is reached.
fn jsonish() -> impl Strategy<Value = Vec<u8>> {
    let token = prop_oneof![
        Just(&b"{"[..]),
        Just(&b"}"[..]),
        Just(&b"["[..]),
        Just(&b"]"[..]),
        Just(&b":"[..]),
        Just(&b","[..]),
        Just(&b"\"jsonrpc\""[..]),
        Just(&b"\"2.0\""[..]),
        Just(&b"\"id\""[..]),
        Just(&b"1"[..]),
        Just(&b"-0"[..]),
        Just(&b"1.5"[..]),
        Just(&b"\"method\""[..]),
        Just(&b"\"m\""[..]),
        Just(&b"\"params\""[..]),
        Just(&b"null"[..]),
        Just(&b"\"result\""[..]),
        Just(&b"\"error\""[..]),
        Just(&b"{\"code\":1,\"message\":\"x\"}"[..]),
        Just(&b"\""[..]),
        Just(&b"\\"[..]),
        Just(&b" "[..]),
        Just(&b"\n"[..]),
        Just(&b"\xff"[..]),
        Just(&b"\"$/cancelRequest\""[..]),
        Just(&b"{\"id\":1}"[..]),
    ];
    proptest::collection::vec(token, 0..40).prop_map(|tokens| tokens.concat())
}

fn hostile_router() -> Router<Vec<Responder>> {
    Router::<Vec<Responder>>::new()
        .request("m", |_, params, _| params.parse().map(|v| v.into_owned()))
        .request("boom", |_, _, _| -> Result<(), ErrorObject> {
            panic!("deliberate")
        })
        .deferred("later", |held, _, _, responder| held.push(responder))
        .notification("m", |_, params, _| {
            let _ = params.parse();
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn prop_decoder_never_panics_and_stays_bounded(
        input in prop_oneof![proptest::collection::vec(any::<u8>(), 0..800), jsonish()],
        chunk in 1usize..100,
        newline in any::<bool>(),
    ) {
        let framing = if newline { Framing::Newline } else { Framing::ContentLength };
        let limits = Limits::new().with_max_message_bytes(64).with_max_header_bytes(64).with_max_headers(4);
        let mut decoder = framing.decoder(&limits);
        let mut rest = input.as_slice();
        'outer: loop {
            loop {
                match decoder.next_frame() {
                    Ok(Some(frame)) => prop_assert!(frame.len() <= 64),
                    Ok(None) => break,
                    Err(error) if error.is_recoverable() => {}
                    Err(_) => break 'outer,
                }
            }
            prop_assert!(decoder.buffered() <= decoder.memory_bound());
            if rest.is_empty() {
                let _ = decoder.finish();
                break;
            }
            let n = decoder.feed(&rest[..rest.len().min(chunk)]);
            prop_assert!(n > 0);
            rest = &rest[n..];
        }
    }

    #[test]
    fn prop_decode_never_panics_and_uses_spec_codes(
        input in prop_oneof![proptest::collection::vec(any::<u8>(), 0..300), jsonish()],
    ) {
        let check = |entry: &rpc_io::Entry<'_>| match entry {
            Err(invalid) => {
                let code = invalid.error().code();
                code == ErrorCode::PARSE_ERROR || code == ErrorCode::INVALID_REQUEST
            }
            Ok(_) => true,
        };
        match decode(&input, &Limits::new()) {
            Incoming::Single(entry) => prop_assert!(check(&entry)),
            Incoming::Batch(entries) => {
                prop_assert!(!entries.is_empty());
                prop_assert!(entries.iter().all(check));
            }
        }
    }

    #[test]
    fn prop_endpoint_never_panics_and_only_sends_responses(
        bodies in proptest::collection::vec(prop_oneof![proptest::collection::vec(any::<u8>(), 0..120), jsonish()], 1..6),
    ) {
        quiet_deliberate_panics();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let mut endpoint = Endpoint::new(tx, hostile_router(), Vec::new())
            .with_config(Config::new().with_limits(Limits::new().with_max_batch(8)));
        for body in &bodies {
            prop_assert!(endpoint.handle(body).is_ok());
        }
        endpoint.state_mut().clear(); // answers every deferred request
        for reply in sent(&rx) {
            match decode(reply.as_bytes(), &Limits::new()) {
                Incoming::Single(Ok(Message::Response(_))) => {}
                Incoming::Batch(entries) => {
                    prop_assert!(entries.iter().all(|e| matches!(e, Ok(Message::Response(_)))), "{}", reply);
                }
                other => prop_assert!(false, "sent a non-response {:?}: {}", other, reply),
            }
        }
    }
}

// --- Classification against a reference -----------------------------------------

/// What a message body should decode to, by the rules `decode` documents.
#[derive(Debug, PartialEq)]
enum Class {
    Request {
        id: String,
        method: String,
        params: Option<String>,
    },
    Notification {
        method: String,
        params: Option<String>,
    },
    Response {
        id: Option<String>,
        ok: bool,
    },
    /// A request-like message answered with `code` under `id` (`None`: null).
    Refused {
        id: Option<String>,
        code: i64,
    },
    /// A malformed response, never answered.
    Dropped,
}

enum RefId {
    Valid(String),
    Null,
    Invalid,
}

fn ref_id(value: &Value<'_>) -> RefId {
    match value {
        Value::String(s) => RefId::Valid(format!("s:{s}")),
        Value::Null => RefId::Null,
        Value::Number(n) => n
            .as_i64()
            .map_or(RefId::Invalid, |n| RefId::Valid(format!("n:{n}"))),
        _ => RefId::Invalid,
    }
}

fn ref_error_ok(value: &Value<'_>) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let mut counts = BTreeMap::new();
    for (key, _) in object.as_slice() {
        *counts.entry(key.as_ref()).or_insert(0) += 1;
    }
    if ["code", "message", "data"]
        .iter()
        .any(|k| counts.get(k).copied().unwrap_or(0) > 1)
    {
        return false;
    }
    object.get("code").and_then(Value::as_i64).is_some()
        && object.get("message").and_then(Value::as_str).is_some()
}

/// The reference: a full parse that keeps duplicate members, then the
/// envelope rules applied to the tree.
fn reference(body: &str) -> Class {
    let parser = Parser::new().duplicate_keys(DuplicateKeys::KeepAll);
    let Ok(doc) = parser.parse(body) else {
        return Class::Refused {
            id: None,
            code: -32700,
        };
    };
    let Some(object) = doc.as_object() else {
        return Class::Refused {
            id: None,
            code: -32600,
        };
    };
    let envelope = ["jsonrpc", "id", "method", "params", "result", "error"];
    let mut members: BTreeMap<&str, Vec<&Value<'_>>> = BTreeMap::new();
    for (key, value) in object.as_slice() {
        if envelope.contains(&key.as_ref()) {
            members.entry(key.as_ref()).or_default().push(value);
        }
    }
    let first = |k: &str| members.get(k).and_then(|v| v.first().copied());
    let duplicate = members.values().any(|v| v.len() > 1);
    let id_repeated = members.get("id").is_some_and(|v| v.len() > 1);
    let id = first("id").map(ref_id);
    let valid_id = match &id {
        Some(RefId::Valid(s)) if !id_repeated => Some(s.clone()),
        _ => None,
    };
    let version_ok = first("jsonrpc").and_then(Value::as_str) == Some("2.0");
    let refused = |id: Option<String>| Class::Refused { id, code: -32600 };
    if let Some(method) = first("method") {
        if duplicate || !version_ok {
            return refused(valid_id);
        }
        let Some(method) = method.as_str() else {
            return refused(valid_id);
        };
        if first("result").is_some() || first("error").is_some() {
            return refused(valid_id);
        }
        let params = match first("params") {
            None | Some(Value::Null) => None,
            Some(p @ (Value::Array(_) | Value::Object(_))) => Some(jsonx_lib::to_string(p)),
            Some(_) => return refused(valid_id),
        };
        return match id {
            None => Class::Notification {
                method: method.to_owned(),
                params,
            },
            Some(RefId::Valid(id)) => Class::Request {
                id,
                method: method.to_owned(),
                params,
            },
            Some(RefId::Null | RefId::Invalid) => refused(valid_id),
        };
    }
    let (result, error) = (first("result"), first("error"));
    if result.is_none() && error.is_none() {
        return refused(None);
    }
    if duplicate || !version_ok || (result.is_some() && error.is_some()) {
        return Class::Dropped;
    }
    if let Some(error) = error {
        if !ref_error_ok(error) {
            return Class::Dropped;
        }
    }
    match id {
        None | Some(RefId::Invalid) => Class::Dropped,
        Some(RefId::Null) if result.is_some() => Class::Dropped,
        Some(RefId::Null) => Class::Response {
            id: None,
            ok: false,
        },
        Some(RefId::Valid(id)) => Class::Response {
            id: Some(id),
            ok: result.is_some(),
        },
    }
}

fn id_key(id: &Id<'_>) -> String {
    match id {
        Id::Number(n) => format!("n:{n}"),
        Id::String(s) => format!("s:{s}"),
    }
}

/// What `decode` made of `body`, in the reference's terms.
fn actual(body: &str) -> Class {
    let keep_all = Parser::new().duplicate_keys(DuplicateKeys::KeepAll);
    let compact = |raw: &Raw<'_>| {
        raw.parse_with(&keep_all)
            .map(|v| jsonx_lib::to_string(&v))
            .unwrap_or_default()
    };
    match decode(body.as_bytes(), &Limits::new()) {
        Incoming::Single(Ok(Message::Request(r))) => Class::Request {
            id: id_key(&r.id),
            method: r.method.into_owned(),
            params: r.params.as_ref().map(compact),
        },
        Incoming::Single(Ok(Message::Notification(n))) => Class::Notification {
            method: n.method.into_owned(),
            params: n.params.as_ref().map(compact),
        },
        Incoming::Single(Ok(Message::Response(r))) => Class::Response {
            id: r.id.as_ref().map(id_key),
            ok: r.result.is_ok(),
        },
        Incoming::Single(Err(invalid)) if invalid.is_response() => Class::Dropped,
        Incoming::Single(Err(invalid)) => Class::Refused {
            id: invalid.id().map(id_key),
            code: invalid.error().code().0,
        },
        Incoming::Batch(_) => Class::Refused { id: None, code: 0 },
    }
}

fn envelope() -> impl Strategy<Value = String> {
    let member = prop_oneof![
        prop_oneof![
            Just(r#""2.0""#),
            Just(r#""1.0""#),
            Just("2.0"),
            Just("null"),
            Just(r#""2.0""#)
        ]
        .prop_map(|v| format!(r#""jsonrpc":{v}"#)),
        prop_oneof![
            Just("1"),
            Just("-7"),
            Just("0"),
            Just(r#""a""#),
            Just(r#""a\"b""#),
            Just("null"),
            Just("1.5"),
            Just("1e2"),
            Just("[1]"),
            Just("{}"),
            Just("true"),
            Just("99999999999999999999"),
        ]
        .prop_map(|v| format!(r#""id":{v}"#)),
        prop_oneof![Just(r#""m""#), Just(r#""a\nb""#), Just("1"), Just("null")]
            .prop_map(|v| format!(r#""method":{v}"#)),
        prop_oneof![
            Just("[]"),
            Just(r#"[1,{"a":2}]"#),
            Just("{}"),
            Just(r#"{"k":"v","k":1}"#),
            Just("null"),
            Just("5"),
            Just(r#""s""#),
        ]
        .prop_map(|v| format!(r#""params":{v}"#)),
        prop_oneof![Just("null"), Just("1"), Just(r#"{"a":[]}"#)]
            .prop_map(|v| format!(r#""result":{v}"#)),
        prop_oneof![
            Just(r#"{"code":-32601,"message":"x"}"#),
            Just(r#"{"code":1,"message":"x","data":[1]}"#),
            Just(r#"{"code":1.5,"message":"x"}"#),
            Just(r#"{"message":"x"}"#),
            Just(r#""oops""#),
            Just(r#"{"code":1,"message":2}"#),
            Just(r#"{"code":1,"message":"a","code":2}"#),
            Just(r#"{"code":1,"message":"a","extra":true}"#),
        ]
        .prop_map(|v| format!(r#""error":{v}"#)),
        Just(r#""x":[1,2]"#.to_owned()),
    ];
    proptest::collection::vec(member, 0..6).prop_map(|members| format!("{{{}}}", members.join(",")))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn prop_classification_matches_the_reference(body in envelope()) {
        prop_assert_eq!(actual(&body), reference(&body), "{}", body);
    }
}

// --- Batches: one response per request, ids matched ---------------------------

#[derive(Clone, Debug)]
enum Part {
    /// A request with this method; its id comes from its position.
    Call {
        method: &'static str,
        string_id: bool,
    },
    Note(&'static str),
    /// A malformed element, answered under `null`.
    Bad(&'static str),
    /// A response from the peer: never answered.
    Answer,
}

fn part() -> impl Strategy<Value = Part> {
    prop_oneof![
        4 => (prop_oneof![Just("echo"), Just("fail"), Just("boom"), Just("missing"), Just("later"), Just("cancelled")], any::<bool>())
            .prop_map(|(method, string_id)| Part::Call { method, string_id }),
        2 => prop_oneof![Just("echo"), Just("missing"), Just("boom")].prop_map(Part::Note),
        1 => prop_oneof![Just("1"), Just("[]"), Just(r#"{"jsonrpc":"2.0","method":1}"#), Just("{}")].prop_map(Part::Bad),
        1 => Just(Part::Answer),
    ]
}

fn batch_router() -> Router<Vec<(String, Responder)>> {
    Router::<Vec<(String, Responder)>>::new()
        .request("echo", |_, params, _| {
            Ok(params.into_raw().map_or(Raw::NULL, Raw::into_owned))
        })
        .request("fail", |_, _, _| {
            Err::<(), _>(ErrorObject::new(ErrorCode(1001), "no"))
        })
        .request("boom", |_, _, _| -> Result<(), ErrorObject> {
            panic!("deliberate")
        })
        .deferred("later", |held, _, cx, responder| {
            held.push((cx.method().to_owned(), responder))
        })
        .deferred("cancelled", |held, _, cx, responder| {
            held.push((cx.method().to_owned(), responder))
        })
        .notification("echo", |_, _, _| {})
        .notification("boom", |_, _, _| panic!("deliberate"))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_batch_responses_match_request_ids(parts in proptest::collection::vec(part(), 1..12)) {
        quiet_deliberate_panics();
        let (tx, rx) = std::sync::mpsc::channel::<String>();
        let mut endpoint = Endpoint::new(tx, batch_router(), Vec::new());
        let mut elements = Vec::new();
        // id key -> the code expected for it (0: a result).
        let mut expected: BTreeMap<String, i64> = BTreeMap::new();
        let mut nulls = 0usize;
        let mut cancels = Vec::new();
        for (i, part) in parts.iter().enumerate() {
            match part {
                Part::Call { method, string_id } => {
                    let (id_text, key) = if *string_id { (format!("\"r{i}\""), format!("s:r{i}")) } else { (i.to_string(), format!("n:{i}")) };
                    elements.push(format!(r#"{{"jsonrpc":"2.0","id":{id_text},"method":"{method}","params":[{i}]}}"#));
                    let code = match *method {
                        "echo" | "later" => 0,
                        "fail" => 1001,
                        "boom" => -32603,
                        "missing" => -32601,
                        _ => -32800,
                    };
                    if *method == "cancelled" {
                        cancels.push(format!(r#"{{"jsonrpc":"2.0","method":"$/cancelRequest","params":{{"id":{id_text}}}}}"#));
                    }
                    expected.insert(key, code);
                }
                Part::Note(method) => elements.push(format!(r#"{{"jsonrpc":"2.0","method":"{method}"}}"#)),
                Part::Bad(text) => {
                    elements.push((*text).to_owned());
                    nulls += 1;
                }
                Part::Answer => elements.push(r#"{"jsonrpc":"2.0","id":9999,"result":1}"#.to_owned()),
            }
        }
        elements.extend(cancels);
        let body = format!("[{}]", elements.join(","));
        prop_assert!(endpoint.handle(body.as_bytes()).is_ok());
        // Answer the deferred requests, last first.
        let held = std::mem::take(endpoint.state_mut());
        for (method, responder) in held.into_iter().rev() {
            let result = responder.ok(method.as_str());
            prop_assert_eq!(result.is_ok(), method == "later");
        }
        let replies = sent(&rx);
        if expected.is_empty() && nulls == 0 {
            prop_assert!(replies.is_empty(), "{:?}", replies);
            return Ok(());
        }
        prop_assert_eq!(replies.len(), 1, "{:?}", replies);
        let Incoming::Batch(entries) = decode(replies[0].as_bytes(), &Limits::new()) else {
            return Err(TestCaseError::fail(format!("not a batch: {}", replies[0])));
        };
        let mut got: BTreeMap<String, i64> = BTreeMap::new();
        let mut got_nulls = 0usize;
        for entry in entries {
            let Ok(Message::Response(response)) = entry else {
                return Err(TestCaseError::fail("a batch element is not a response"));
            };
            let code = response.result.as_ref().err().map_or(0, |e| e.code().0);
            match response.id {
                None => {
                    prop_assert_eq!(code, -32600);
                    got_nulls += 1;
                }
                Some(id) => prop_assert!(got.insert(id_key(&id), code).is_none(), "an id answered twice"),
            }
        }
        prop_assert_eq!(got, expected);
        prop_assert_eq!(got_nulls, nulls);
    }
}

#[test]
fn test_reference_spots_checks() {
    // Sanity checks on the reference itself.
    assert_eq!(
        reference("{"),
        Class::Refused {
            id: None,
            code: -32700
        }
    );
    assert_eq!(
        reference(r#"{"jsonrpc":"2.0","method":"m"}"#),
        Class::Notification {
            method: "m".into(),
            params: None
        }
    );
    assert_eq!(
        reference(r#"{"jsonrpc":"2.0","id":1,"method":"m","params":3}"#),
        Class::Refused {
            id: Some("n:1".into()),
            code: -32600
        }
    );
    assert_eq!(
        reference(r#"{"jsonrpc":"2.0","id":null,"result":1}"#),
        Class::Dropped
    );
    let _ = FrameError::Truncated;
}
