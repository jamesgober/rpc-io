//! The `serde` feature: typed params and results in handlers and calls.
#![cfg(feature = "serde")]

mod common;

use std::collections::BTreeMap;

use common::{endpoint, sent};
use rpc_io::{CallError, Error, ErrorCode, Event, Id, Responder, Router};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct HoverParams {
    text_document: Document,
    position: Position,
}

#[derive(Debug, Deserialize, PartialEq)]
struct Document {
    uri: String,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct Position {
    line: u32,
    character: u32,
}

#[derive(Serialize)]
struct Hover {
    contents: String,
}

fn router() -> Router<Vec<String>> {
    Router::<Vec<String>>::new()
        .request_typed("textDocument/hover", |_, p: HoverParams, _| {
            Ok(Hover {
                contents: format!("{}:{}", p.text_document.uri, p.position.line),
            })
        })
        .request_typed("unwritable", |_, (): (), _| {
            let mut map = BTreeMap::new();
            let _none = map.insert((1, 2), "tuple keys are not JSON");
            Ok(map)
        })
        .request_typed("refuse", |_, (): (), _| {
            Err::<(), _>(rpc_io::ErrorObject::new(ErrorCode(7), "no"))
        })
        .notification_typed("moved", |log, p: Position, _| {
            log.push(format!("{}:{}", p.line, p.character));
        })
        .on_event(|log, event| {
            if let Event::InvalidParams { method, error } = event {
                log.push(format!("invalid {method} {}", error.code().0));
            }
        })
}

#[test]
fn test_typed_request_reads_params_and_writes_the_result() {
    let (mut e, rx) = endpoint(router(), Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"file:///a.rs"},"position":{"line":3,"character":9}}}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","id":1,"result":{"contents":"file:///a.rs:3"}}"#]
    );
}

#[test]
fn test_typed_request_refuses_params_that_do_not_fit() {
    let (mut e, rx) = endpoint(router(), Vec::new());
    for params in [
        r#""params":{"textDocument":{"uri":5},"position":{"line":3,"character":9}}"#,
        r#""params":[1,2]"#,
        r#""params":null"#,
    ] {
        let body = format!(r#"{{"jsonrpc":"2.0","id":2,"method":"textDocument/hover",{params}}}"#);
        assert!(e.handle(body.as_bytes()).is_ok());
        let reply = sent(&rx);
        assert!(
            reply[0].starts_with(
                r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32602,"message":"invalid params: "#
            ),
            "{reply:?}"
        );
    }
}

#[test]
fn test_typed_request_errors_and_unwritable_results() {
    let (mut e, rx) = endpoint(router(), Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":3,"method":"refuse"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":4,"method":"unwritable"}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert_eq!(
        replies[0],
        r#"{"jsonrpc":"2.0","id":3,"error":{"code":7,"message":"no"}}"#
    );
    assert!(
        replies[1].starts_with(
            r#"{"jsonrpc":"2.0","id":4,"error":{"code":-32603,"message":"the value is not JSON: "#
        ),
        "{replies:?}"
    );
}

#[test]
fn test_typed_notification_reports_params_that_do_not_fit() {
    let (mut e, rx) = endpoint(router(), Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"moved","params":{"line":1,"character":2}}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"moved","params":{"line":"one"}}"#)
            .is_ok()
    );
    assert_eq!(e.state(), &["1:2", "invalid moved -32602"]);
    assert!(sent(&rx).is_empty(), "a notification was answered");
}

#[test]
fn test_typed_calls_both_ways() {
    let (mut e, rx) = endpoint(Router::<()>::new(), ());
    let peer = e.peer();
    let call = peer.request_typed(
        "position",
        &Position {
            line: 1,
            character: 2,
        },
    );
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","id":1,"method":"position","params":{"line":1,"character":2}}"#]
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"result":{"line":5,"character":6}}"#)
            .is_ok()
    );
    assert_eq!(
        call.wait_typed::<Position>(),
        Ok(Position {
            line: 5,
            character: 6
        })
    );
    // A result of the wrong shape is an invalid response.
    let call = peer.request_typed("position", &());
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"result":"nope"}"#)
            .is_ok()
    );
    assert!(matches!(
        call.wait_typed::<Position>(),
        Err(CallError::InvalidResponse(error)) if error.code() == ErrorCode::INVALID_PARAMS
    ));
    // Params that are not JSON are refused before anything is sent.
    let mut keys = BTreeMap::new();
    let _none = keys.insert((1, 2), 3);
    assert!(matches!(
        peer.notify_typed("x", &keys),
        Err(Error::Serialize(_))
    ));
    assert!(matches!(
        peer.request_typed("x", &keys),
        Err(Error::Serialize(_))
    ));
    assert!(matches!(
        peer.notify_typed("x", &5),
        Err(Error::InvalidParams(_))
    ));
    let _request_and_nothing_more = sent(&rx);
    assert!(
        Error::Serialize("why".into())
            .to_string()
            .starts_with("the value is not JSON: why")
    );
}

#[test]
fn test_typed_answer_that_cannot_be_written_still_answers_once() {
    let router = Router::<Vec<Responder>>::new().deferred("later", |held, _, _, r| held.push(r));
    let (mut e, rx) = endpoint(router, Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":"a","method":"later"}"#)
            .is_ok()
    );
    let Some(responder) = e.state_mut().pop() else {
        panic!("no responder")
    };
    let mut keys = BTreeMap::new();
    let _none = keys.insert((1, 2), 3);
    assert!(matches!(
        responder.ok_typed(&keys),
        Err(Error::Serialize(_))
    ));
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert!(replies[0].starts_with(r#"{"jsonrpc":"2.0","id":"a","error":{"code":-32603"#));
}

#[test]
fn test_ids_and_codes_through_serde() {
    #[derive(Deserialize, Serialize)]
    struct Cancel {
        id: Id<'static>,
        code: ErrorCode,
    }
    let parsed: Result<Cancel, _> = rpc_io::Raw::new(r#"{"id":"r-1","code":-32800}"#)
        .map_err(|e| e.to_string())
        .and_then(|raw| raw.deserialize::<Cancel>().map_err(|e| e.to_string()));
    let Ok(cancel) = parsed else {
        panic!("{parsed:?}", parsed = parsed.err())
    };
    assert_eq!(cancel.id, Id::from("r-1"));
    assert_eq!(cancel.code, ErrorCode::REQUEST_CANCELLED);
    let json = rpc_io::Json::serialize(&cancel).map(|j| j.to_string());
    assert_eq!(json.ok().as_deref(), Some(r#"{"id":"r-1","code":-32800}"#));
}

#[test]
fn test_a_mismatch_quoting_a_huge_value_is_clipped() {
    // Found by the verifier (RPC2-005): serde's message quotes the value
    // that did not fit, so a 100,000-byte string came back whole.
    let (mut e, rx) = endpoint(router(), Vec::new());
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"textDocument/hover","params":{{"textDocument":{{"uri":7}},"position":"{}"}}}}"#,
        "A".repeat(100_000)
    );
    assert!(e.handle(body.as_bytes()).is_ok());
    let replies = sent(&rx);
    assert!(replies[0].len() < 600, "{} bytes", replies[0].len());
    assert!(replies[0].contains("-32602"));
}
