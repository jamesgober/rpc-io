//! Duplicate keys in the envelope (the jsonx-lib baseline audit, JX-SEC-02):
//! a repeated `method`, `id`, `params`, `jsonrpc`, `result`, or `error` must
//! never let one reader see one value and another reader a different one, so
//! no method or id can be smuggled past the gate, the interceptor, or the
//! router. Every such message is refused with `INVALID_REQUEST`.
#![cfg(feature = "std")]

mod common;

use common::{endpoint, sent};
use rpc_io::{Config, ErrorCode, Gate, Incoming, Limits, Message, Router, decode};

/// A router that records every method it was asked to run.
fn recording() -> Router<Vec<String>> {
    Router::new()
        .request("initialize", |seen: &mut Vec<String>, _, _| {
            seen.push("initialize".to_owned());
            Ok(())
        })
        .request("shutdown", |seen: &mut Vec<String>, _, _| {
            seen.push("shutdown".to_owned());
            Ok(())
        })
        .notification("exit", |seen: &mut Vec<String>, _, _| {
            seen.push("exit".to_owned());
        })
}

#[test]
fn test_repeated_method_is_refused_and_runs_nothing() {
    let (e, rx) = endpoint(recording(), Vec::new());
    let mut e = e.with_config(Config::new().with_gate(Some(Gate::new("initialize"))));
    // The audit's example: two readers that disagree would see `initialize`
    // (opening the gate) and `shutdown` (running past it).
    let body = br#"{"jsonrpc":"2.0","method":"initialize","method":"shutdown","id":1}"#;
    assert!(e.handle(body).is_ok());
    assert!(e.state().is_empty(), "ran {:?}", e.state());
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32600"#),
        "{}",
        replies[0]
    );
    assert!(replies[0].contains("`method` appears more than once"));
    // The gate is still shut: a later request is refused, not served.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"shutdown","id":2}"#)
            .is_ok()
    );
    assert!(e.state().is_empty());
    assert!(sent(&rx)[0].contains("-32002"));
}

#[test]
fn test_every_repeated_envelope_member_is_refused() {
    for body in [
        r#"{"jsonrpc":"2.0","jsonrpc":"2.0","method":"shutdown","id":1}"#,
        r#"{"jsonrpc":"2.0","method":"shutdown","id":1,"id":2}"#,
        r#"{"jsonrpc":"2.0","method":"shutdown","params":[],"params":{},"id":1}"#,
        r#"{"jsonrpc":"2.0","method":"exit","method":"exit"}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":1,"result":2}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"a"},"error":{"code":2,"message":"b"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"code":2,"message":"a"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"a","message":"b"}}"#,
    ] {
        let Incoming::Single(Err(invalid)) = decode(body.as_bytes(), &Limits::new()) else {
            panic!("accepted {body}");
        };
        assert_eq!(invalid.error().code(), ErrorCode::INVALID_REQUEST, "{body}");
    }
    // A repeated id answers nothing under either id.
    let body = br#"{"jsonrpc":"2.0","method":"shutdown","id":1,"id":2}"#;
    let Incoming::Single(Err(invalid)) = decode(body, &Limits::new()) else {
        panic!("accepted");
    };
    assert!(invalid.id().is_none());
    // Members outside the envelope may repeat: they are not read.
    let body = br#"{"jsonrpc":"2.0","method":"exit","x":1,"x":2}"#;
    assert!(matches!(
        decode(body, &Limits::new()),
        Incoming::Single(Ok(Message::Notification(_)))
    ));
}

#[test]
fn test_repeated_members_inside_a_batch_are_refused_one_by_one() {
    let (mut e, rx) = endpoint(recording(), Vec::new());
    let body = br#"[{"jsonrpc":"2.0","method":"initialize","method":"shutdown","id":1},{"jsonrpc":"2.0","method":"initialize","id":2}]"#;
    assert!(e.handle(body).is_ok());
    assert_eq!(e.state(), &["initialize"]);
    let reply = sent(&rx).concat();
    assert!(
        reply.contains(r#""id":1,"error":{"code":-32600"#),
        "{reply}"
    );
    assert!(reply.contains(r#""id":2,"result":null"#), "{reply}");
}

#[test]
fn test_repeated_members_in_cancel_and_progress_targets_name_nothing() {
    use rpc_io::Responder;

    let router = Router::new().deferred("slow", |kept: &mut Vec<Responder>, params, _, r| {
        // A repeated progress token names no token.
        assert!(params.progress_token().is_none());
        kept.push(r);
    });
    let (e, rx) = endpoint(router, Vec::new());
    let mcp = Config::new().with_cancellation(Some(rpc_io::Cancellation::mcp()));
    let mut e = e.with_config(mcp);
    for id in [1, 2] {
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"slow","params":{{"_meta":{{"progressToken":1,"progressToken":2}}}}}}"#
        );
        assert!(e.handle(body.as_bytes()).is_ok());
    }
    // A cancel naming two requests cancels neither.
    let cancel = br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"requestId":2}}"#;
    assert!(e.handle(cancel).is_ok());
    assert!(e.state().iter().all(|r| !r.is_cancelled()));
    assert!(sent(&rx).is_empty());
}
