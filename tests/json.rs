//! The typed API through jsonx-lib's `ToJson`: results encoded into the
//! endpoint's buffer, typed params and deferred answers through the
//! per-thread buffer, and the edge cases of both (panics, nesting, size).
#![cfg(feature = "std")]

mod common;

use std::panic::{self, AssertUnwindSafe};

use common::{endpoint, quiet_deliberate_panics, sent};
use jsonx_lib::{ToJson, ValueWriter, Written};
use rpc_io::{Endpoint, Error, ErrorCode, Id, Peer, Responder, Router};

struct Hover<'a> {
    kind: &'a str,
    value: u64,
    note: Option<&'a str>,
}

impl ToJson for Hover<'_> {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.object(|o| {
            o.member_with("contents", |c| {
                c.object(|c| {
                    c.member("kind", self.kind);
                    c.member("value", &self.value);
                })
            });
            o.optional("note", &self.note);
        })
    }
}

#[test]
fn test_request_encoded_writes_the_result_as_encoded() {
    let router = Router::new()
        .request_encoded("hover", |_: &mut (), params, _| {
            let p = params.parse()?;
            Ok(Hover {
                kind: "markdown",
                value: p["line"].as_u64().unwrap_or(0),
                note: None,
            })
        })
        .request_encoded("fail", |_: &mut (), _, _| {
            Err::<(), _>(rpc_io::ErrorObject::new(ErrorCode(7), "no"))
        });
    let (mut e, rx) = endpoint(router, ());
    for line in [3, 4] {
        let body = format!(
            r#"{{"jsonrpc":"2.0","id":{line},"method":"hover","params":{{"line":{line}}}}}"#
        );
        assert!(e.handle(body.as_bytes()).is_ok());
    }
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":9,"method":"fail"}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","id":3,"result":{"contents":{"kind":"markdown","value":3}}}"#,
            r#"{"jsonrpc":"2.0","id":4,"result":{"contents":{"kind":"markdown","value":4}}}"#,
            r#"{"jsonrpc":"2.0","id":9,"error":{"code":7,"message":"no"}}"#,
        ]
    );
}

#[test]
fn test_request_encoded_in_a_batch_and_with_a_large_result() {
    let router = Router::new().request_encoded("blob", |_: &mut (), params, _| {
        let p = params.parse()?;
        let n = usize::try_from(p[0].as_u64().unwrap_or(0)).unwrap_or(0);
        Ok("x".repeat(n))
    });
    let (mut e, rx) = endpoint(router, ());
    // Larger than the buffer the endpoint keeps, then small again.
    let body = br#"[{"jsonrpc":"2.0","id":1,"method":"blob","params":[100000]},{"jsonrpc":"2.0","id":2,"method":"blob","params":[2]}]"#;
    assert!(e.handle(body).is_ok());
    let reply = sent(&rx).concat();
    let envelope = r#"{"jsonrpc":"2.0","id":1,"result":""}"#.len();
    assert_eq!(reply.len(), 100_000 + 2 + 2 * envelope + "[,]".len());
    assert!(reply.ends_with(r#"{"jsonrpc":"2.0","id":2,"result":"xx"}]"#));
}

struct Bomb;

impl ToJson for Bomb {
    fn to_json(&self, _out: ValueWriter<'_>) -> Written {
        panic!("deliberate: the encoding panicked");
    }
}

#[test]
fn test_a_panic_while_encoding_a_result_is_contained() {
    quiet_deliberate_panics();
    let router = Router::new()
        .request_encoded("bomb", |_: &mut (), _, _| Ok(Bomb))
        .request_encoded("ok", |_: &mut (), _, _| Ok(1));
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"bomb"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"ok"}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert!(
        replies[0].starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603"#),
        "{}",
        replies[0]
    );
    assert_eq!(replies[1], r#"{"jsonrpc":"2.0","id":2,"result":1}"#);
}

#[test]
fn test_typed_notifications_requests_and_answers() {
    let router = Router::new()
        .deferred("later", |kept: &mut Vec<Responder>, _, _, r| kept.push(r))
        .notification("ping", |_, _, cx| {
            assert!(
                cx.notify_json("pong", &[Id::from(1), Id::from("a")])
                    .is_ok()
            );
            assert!(cx.request_json("ask", &()).is_ok_and(|call| {
                call.detach();
                true
            }));
        });
    let (mut e, rx) = endpoint(router, Vec::new());
    assert!(e.handle(br#"{"jsonrpc":"2.0","method":"ping"}"#).is_ok());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":5,"method":"later"}"#)
            .is_ok()
    );
    let kept = std::mem::take(e.state_mut());
    for r in kept {
        assert!(
            r.ok_json(&Hover {
                kind: "plaintext",
                value: 1,
                note: Some("n")
            })
            .is_ok()
        );
    }
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","method":"pong","params":[1,"a"]}"#,
            r#"{"jsonrpc":"2.0","id":1,"method":"ask"}"#,
            r#"{"jsonrpc":"2.0","id":5,"result":{"contents":{"kind":"plaintext","value":1},"note":"n"}}"#,
        ]
    );
    // Params that are not an array, an object, or null are refused.
    let peer = e.peer();
    assert!(matches!(
        peer.notify_json("n", &5),
        Err(Error::InvalidParams(_))
    ));
    assert!(matches!(
        peer.request_json("r", "s"),
        Err(Error::InvalidParams(_))
    ));
    assert!(sent(&rx).is_empty());
}

/// A value whose encoding sends another typed value through the same peer:
/// the per-thread buffer is in use, so the inner one takes a fresh buffer.
struct Nested(Peer);

impl ToJson for Nested {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        let _sent = self.0.notify_json("inner", &["i"]);
        out.array(|a| a.item("outer"))
    }
}

#[test]
fn test_encoding_inside_an_encoding_uses_a_fresh_buffer() {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let e = Endpoint::new(tx, Router::<()>::new(), ());
    let peer = e.peer();
    assert!(peer.notify_json("outer", &Nested(e.peer())).is_ok());
    assert_eq!(
        rx.try_iter().collect::<Vec<_>>(),
        [
            r#"{"jsonrpc":"2.0","method":"inner","params":["i"]}"#,
            r#"{"jsonrpc":"2.0","method":"outer","params":["outer"]}"#,
        ]
    );
}

#[test]
fn test_a_panic_while_encoding_params_leaves_the_buffer_usable() {
    quiet_deliberate_panics();
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let e = Endpoint::new(tx, Router::<()>::new(), ());
    let peer = e.peer();
    let caught = panic::catch_unwind(AssertUnwindSafe(|| peer.notify_json("n", &[Bomb])));
    assert!(caught.is_err());
    assert!(peer.notify_json("n", &[2]).is_ok());
    assert_eq!(
        rx.try_iter().collect::<Vec<_>>(),
        [r#"{"jsonrpc":"2.0","method":"n","params":[2]}"#]
    );
}

#[test]
fn test_typed_answers_from_other_threads() {
    let router = Router::new().deferred("work", |_: &mut (), _, _, responder: Responder| {
        let _worker = std::thread::spawn(move || responder.ok_json(&vec![1u8; 3]));
    });
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#)
            .is_ok()
    );
    let reply = rx.recv_timeout(std::time::Duration::from_secs(10)).ok();
    assert_eq!(
        reply.as_deref(),
        Some(r#"{"jsonrpc":"2.0","id":1,"result":[1,1,1]}"#)
    );
}
