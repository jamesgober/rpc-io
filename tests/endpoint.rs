//! The endpoint: routing, the JSON-RPC 2.0 error codes, batches, handler
//! panics, deferred answers, and events.
#![cfg(feature = "std")]

mod common;

use std::sync::{Arc, Mutex, PoisonError, mpsc};

use common::{Faulty, endpoint, quiet_deliberate_panics, sent};
use jsonx_lib::json;
use rpc_io::{
    Config, Endpoint, Error, ErrorCode, ErrorObject, Event, Flow, Limits, Responder, Router,
};

/// The router of JSON-RPC 2.0's own examples (section 7), plus methods that
/// fail, panic, and answer later.
fn spec_router() -> Router<Vec<Responder>> {
    Router::<Vec<Responder>>::new()
        .request("subtract", |_, params, _| {
            let p = params.parse()?;
            let (a, b) = if p.as_array().is_some() {
                (p[0].as_i64(), p[1].as_i64())
            } else {
                (p["minuend"].as_i64(), p["subtrahend"].as_i64())
            };
            match (a, b) {
                (Some(a), Some(b)) => Ok(json!(a - b)),
                _ => Err(ErrorObject::invalid_params("expected two integers")),
            }
        })
        .request("sum", |_, params, _| {
            let p = params.parse()?;
            let total: i64 = p
                .as_array()
                .map_or(0, |a| a.iter().filter_map(|v| v.as_i64()).sum());
            Ok(total)
        })
        .request("get_data", |_, _, _| Ok(json!(["hello", 5])))
        .request("fail", |_, _, _| {
            Err::<(), _>(ErrorObject::new(ErrorCode(1001), "stale version"))
        })
        .request("boom", |_, _, _| -> Result<(), ErrorObject> {
            panic!("deliberate: boom")
        })
        .deferred("later", |held, _, _, responder| held.push(responder))
        .deferred("dropped", |_, _, _, _responder| {})
        .deferred("boom_later", |_, _, _, _responder| {
            panic!("deliberate: boom later")
        })
        .notification("update", |_, _, _| {})
        .notification("notify_hello", |_, _, _| {})
        .notification("notify_boom", |_, _, _| panic!("deliberate: notification"))
}

fn spec() -> (
    Endpoint<Vec<Responder>, mpsc::Sender<String>>,
    mpsc::Receiver<String>,
) {
    quiet_deliberate_panics();
    endpoint(spec_router(), Vec::new())
}

/// Handles `body` and returns everything sent in reply.
fn exchange(
    endpoint: &mut Endpoint<Vec<Responder>, mpsc::Sender<String>>,
    rx: &mpsc::Receiver<String>,
    body: &str,
) -> Vec<String> {
    let flow = endpoint.handle(body.as_bytes());
    assert!(matches!(flow, Ok(Flow::Continue)), "{flow:?}");
    sent(rx)
}

fn one(
    endpoint: &mut Endpoint<Vec<Responder>, mpsc::Sender<String>>,
    rx: &mpsc::Receiver<String>,
    body: &str,
) -> String {
    let mut replies = exchange(endpoint, rx, body);
    assert_eq!(replies.len(), 1, "{body} -> {replies:?}");
    replies.remove(0)
}

// --- JSON-RPC 2.0, section 7, verbatim -------------------------------------

#[test]
fn test_spec_positional_and_named_params() {
    let (mut e, rx) = spec();
    assert_eq!(
        one(
            &mut e,
            &rx,
            r#"{"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": 1}"#
        ),
        r#"{"jsonrpc":"2.0","id":1,"result":19}"#
    );
    assert_eq!(
        one(
            &mut e,
            &rx,
            r#"{"jsonrpc": "2.0", "method": "subtract", "params": {"subtrahend": 23, "minuend": 42}, "id": 3}"#
        ),
        r#"{"jsonrpc":"2.0","id":3,"result":19}"#
    );
}

#[test]
fn test_spec_notifications_get_no_response() {
    let (mut e, rx) = spec();
    assert!(
        exchange(
            &mut e,
            &rx,
            r#"{"jsonrpc": "2.0", "method": "update", "params": [1,2,3,4,5]}"#
        )
        .is_empty()
    );
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc": "2.0", "method": "foobar"}"#).is_empty());
}

#[test]
fn test_spec_method_not_found() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"{"jsonrpc": "2.0", "method": "foobar", "id": "1"}"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":"1","error":{"code":-32601,"#),
        "{reply}"
    );
}

#[test]
fn test_spec_invalid_json() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"{"jsonrpc": "2.0", "method": "foobar, "params": "bar", "baz]"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"#),
        "{reply}"
    );
}

#[test]
fn test_spec_invalid_request_object() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"{"jsonrpc": "2.0", "method": 1, "params": "bar"}"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#),
        "{reply}"
    );
}

#[test]
fn test_spec_batch_invalid_json() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"[
  {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
  {"jsonrpc": "2.0", "method"
]"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"#),
        "{reply}"
    );
}

#[test]
fn test_spec_empty_batch() {
    let (mut e, rx) = spec();
    let reply = one(&mut e, &rx, "[]");
    // One error object, not an array.
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#),
        "{reply}"
    );
}

#[test]
fn test_spec_invalid_batches() {
    let (mut e, rx) = spec();
    let null_error = r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#;
    let reply = one(&mut e, &rx, "[1]");
    assert!(
        reply.starts_with(&format!("[{null_error}")) && reply.ends_with("}]"),
        "{reply}"
    );
    let reply = one(&mut e, &rx, "[1,2,3]");
    assert_eq!(reply.matches(null_error).count(), 3, "{reply}");
    assert!(reply.starts_with('[') && reply.ends_with(']'));
}

#[test]
fn test_spec_mixed_batch() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"[
        {"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},
        {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]},
        {"jsonrpc": "2.0", "method": "subtract", "params": [42,23], "id": "2"},
        {"foo": "boo"},
        {"jsonrpc": "2.0", "method": "foo.get", "params": {"name": "myself"}, "id": "5"},
        {"jsonrpc": "2.0", "method": "get_data", "id": "9"}
    ]"#,
    );
    let ids: Vec<&str> = [
        "\"id\":\"1\",\"result\":7",
        "\"id\":\"2\",\"result\":19",
        "\"id\":null,\"error\":{\"code\":-32600",
        "\"id\":\"5\",\"error\":{\"code\":-32601",
        "\"id\":\"9\",\"result\":[\"hello\",5]",
    ]
    .to_vec();
    for id in ids {
        assert!(reply.contains(id), "{id} missing from {reply}");
    }
    assert_eq!(reply.matches("\"jsonrpc\":\"2.0\"").count(), 5);
}

#[test]
fn test_spec_batch_of_notifications_gets_nothing() {
    let (mut e, rx) = spec();
    let body = r#"[{"jsonrpc": "2.0", "method": "notify_sum", "params": [1,2,4]}, {"jsonrpc": "2.0", "method": "notify_hello", "params": [7]}]"#;
    assert!(exchange(&mut e, &rx, body).is_empty());
}

// --- Error codes -----------------------------------------------------------

#[test]
fn test_invalid_params_is_32602() {
    let (mut e, rx) = spec();
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"subtract","params":["a","b"]}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":"subtract","params":{"minuend":1,"minuend":2}}"#,
    ] {
        let reply = one(&mut e, &rx, body);
        assert!(
            reply.starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"#),
            "{reply}"
        );
    }
}

#[test]
fn test_custom_error_codes_pass_through() {
    let (mut e, rx) = spec();
    assert_eq!(
        one(&mut e, &rx, r#"{"jsonrpc":"2.0","id":4,"method":"fail"}"#),
        r#"{"jsonrpc":"2.0","id":4,"error":{"code":1001,"message":"stale version"}}"#
    );
}

#[test]
fn test_invalid_envelopes_are_answered_under_their_id() {
    let (mut e, rx) = spec();
    for body in [
        r#"{"jsonrpc":"1.0","id":7,"method":"sum"}"#,
        r#"{"id":7,"method":"sum"}"#,
        r#"{"jsonrpc":"2.0","id":7,"method":"sum","params":3}"#,
        r#"{"jsonrpc":"2.0","id":7,"method":"sum","result":3}"#,
        r#"{"jsonrpc":"2.0","id":7,"method":"sum","method":"sum"}"#,
    ] {
        let reply = one(&mut e, &rx, body);
        assert!(
            reply.starts_with(r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32600,"#),
            "{body} -> {reply}"
        );
    }
    // Ids that are not strings or integers are not echoed.
    for id in ["null", "1.5", "[7]", "{}", "true", "1e400"] {
        let body = format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"sum"}}"#);
        let reply = one(&mut e, &rx, &body);
        assert!(
            reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#),
            "{body} -> {reply}"
        );
    }
}

#[test]
fn test_json_budgets_are_invalid_request_with_the_id() {
    quiet_deliberate_panics();
    let limits = Limits::new().with_json(jsonx_lib::Limits::new().with_max_depth(8));
    let (e, rx) = endpoint(spec_router(), Vec::new());
    let mut e = e.with_config(Config::new().with_limits(limits));
    let deep = format!(
        r#"{{"jsonrpc":"2.0","id":"d","method":"sum","params":{}{}}}"#,
        "[".repeat(20),
        "]".repeat(20)
    );
    let reply = one(&mut e, &rx, &deep);
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":"d","error":{"code":-32600,"#),
        "{reply}"
    );
    // A batch over the limit is one error.
    let limits = Limits::new().with_max_batch(2);
    let (e, rx) = endpoint(spec_router(), Vec::new());
    let mut e = e.with_config(Config::new().with_limits(limits));
    let reply = one(
        &mut e,
        &rx,
        r#"[{"jsonrpc":"2.0","id":1,"method":"sum"},{"jsonrpc":"2.0","id":2,"method":"sum"},{"jsonrpc":"2.0","id":3,"method":"sum"}]"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#),
        "{reply}"
    );
}

#[test]
fn test_invalid_utf8_is_a_parse_error() {
    let (mut e, rx) = spec();
    let flow = e.handle(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"s\xffm\"}");
    assert!(flow.is_ok());
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert!(replies[0].contains("-32700"), "{}", replies[0]);
}

// --- Handler panics ----------------------------------------------------------

#[test]
fn test_handler_panic_is_internal_error_and_the_session_survives() {
    let (mut e, rx) = spec();
    assert_eq!(
        one(&mut e, &rx, r#"{"jsonrpc":"2.0","id":1,"method":"boom"}"#),
        r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"message":"the handler panicked"}}"#
    );
    assert_eq!(
        one(
            &mut e,
            &rx,
            r#"{"jsonrpc":"2.0","id":2,"method":"sum","params":[1]}"#
        ),
        r#"{"jsonrpc":"2.0","id":2,"result":1}"#
    );
}

#[test]
fn test_deferred_handler_panic_is_answered_once() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"{"jsonrpc":"2.0","id":1,"method":"boom_later"}"#,
    );
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32603,"#),
        "{reply}"
    );
}

#[test]
fn test_notification_panic_gets_no_response() {
    let (mut e, rx) = spec();
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc":"2.0","method":"notify_boom"}"#).is_empty());
    assert!(exchange(&mut e, &rx, r#"[{"jsonrpc":"2.0","method":"notify_boom"}]"#).is_empty());
}

#[test]
fn test_panics_in_a_batch_are_answered_in_place() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"[{"jsonrpc":"2.0","id":1,"method":"boom"},{"jsonrpc":"2.0","id":2,"method":"sum","params":[2]}]"#,
    );
    assert!(
        reply.contains(r#""id":1,"error":{"code":-32603"#)
            && reply.contains(r#""id":2,"result":2"#),
        "{reply}"
    );
}

#[test]
fn test_panic_is_reported_to_the_event_hook_and_a_panicking_hook_is_contained() {
    quiet_deliberate_panics();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&seen);
    let router = Router::new()
        .request("boom", |_: &mut (), _, _| -> Result<(), ErrorObject> {
            panic!("deliberate: {}", 42)
        })
        .on_event(move |_, event| {
            if let Event::Panicked { method, message } = event {
                log.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(format!("{method}: {message}"));
            }
            panic!("deliberate: the hook panics too");
        });
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"boom"}"#)
            .is_ok()
    );
    assert_eq!(sent(&rx).len(), 1);
    assert_eq!(
        *seen.lock().unwrap_or_else(PoisonError::into_inner),
        ["boom: deliberate: 42"]
    );
}

// --- Deferred answers --------------------------------------------------------

#[test]
fn test_deferred_answer_from_another_thread() {
    let (mut e, rx) = spec();
    assert!(
        exchange(
            &mut e,
            &rx,
            r#"{"jsonrpc":"2.0","id":"w","method":"later"}"#
        )
        .is_empty()
    );
    let responder = e.state_mut().pop();
    let Some(responder) = responder else {
        panic!("no responder")
    };
    std::thread::spawn(move || responder.ok(json!({ "done": true })))
        .join()
        .map_err(|_| "worker panicked")
        .and_then(|r| r.map_err(|_| "send failed"))
        .ok();
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","id":"w","result":{"done":true}}"#]
    );
}

#[test]
fn test_dropped_responder_answers_internal_error() {
    let (mut e, rx) = spec();
    let reply = one(
        &mut e,
        &rx,
        r#"{"jsonrpc":"2.0","id":1,"method":"dropped"}"#,
    );
    assert!(
        reply.contains("-32603") && reply.contains("without answering"),
        "{reply}"
    );
}

#[test]
fn test_deferred_batch_is_sent_when_its_last_answer_arrives() {
    let (mut e, rx) = spec();
    let body = r#"[{"jsonrpc":"2.0","id":1,"method":"later"},{"jsonrpc":"2.0","id":2,"method":"sum","params":[5]},{"jsonrpc":"2.0","id":3,"method":"later"}]"#;
    assert!(exchange(&mut e, &rx, body).is_empty());
    let mut held = std::mem::take(e.state_mut());
    let last = held.pop();
    let first = held.pop();
    assert!(first.map(|r| r.ok("a")).is_some_and(|r| r.is_ok()));
    assert!(
        sent(&rx).is_empty(),
        "the batch went out before its last answer"
    );
    assert!(last.map(|r| r.ok("c")).is_some_and(|r| r.is_ok()));
    assert_eq!(
        sent(&rx),
        [
            r#"[{"jsonrpc":"2.0","id":2,"result":5},{"jsonrpc":"2.0","id":1,"result":"a"},{"jsonrpc":"2.0","id":3,"result":"c"}]"#
        ]
    );
}

#[test]
fn test_id_still_in_flight_is_refused() {
    let (mut e, rx) = spec();
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc":"2.0","id":5,"method":"later"}"#).is_empty());
    let reply = one(&mut e, &rx, r#"{"jsonrpc":"2.0","id":5,"method":"later"}"#);
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":5,"error":{"code":-32600,"#),
        "{reply}"
    );
    // The first request is still the one in flight, and is answered once.
    assert_eq!(e.state().len(), 1);
    let held = e.state_mut().pop();
    assert!(held.map(|r| r.ok(1)).is_some_and(|r| r.is_ok()));
    assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","id":5,"result":1}"#]);
    // Once answered, the id is free again.
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc":"2.0","id":5,"method":"later"}"#).is_empty());
}

#[test]
fn test_max_in_flight_refuses_with_server_overloaded() {
    quiet_deliberate_panics();
    let (e, rx) = endpoint(spec_router(), Vec::new());
    let mut e = e.with_config(Config::new().with_max_in_flight(1));
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc":"2.0","id":1,"method":"later"}"#).is_empty());
    let reply = one(&mut e, &rx, r#"{"jsonrpc":"2.0","id":2,"method":"later"}"#);
    assert!(
        reply.starts_with(r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"#),
        "{reply}"
    );
}

// --- Responses to our requests ----------------------------------------------

#[test]
fn test_responses_settle_calls_and_unknown_ids_are_unmatched() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&events);
    let router = Router::new().on_event(move |_: &mut (), event| {
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("{event:?}"));
    });
    let (mut e, rx) = endpoint(router, ());
    let peer = e.peer();
    let a = peer.request("a", ()).map_err(|e| e.to_string());
    let b = peer.request("b", json!([1])).map_err(|e| e.to_string());
    let (Ok(a), Ok(b)) = (a, b) else {
        panic!("requests failed")
    };
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","id":1,"method":"a"}"#,
            r#"{"jsonrpc":"2.0","id":2,"method":"b","params":[1]}"#
        ]
    );
    // Answers out of order, a repeat, a forged id, a string id, a null-id error.
    for body in [
        r#"{"jsonrpc":"2.0","id":2,"error":{"code":5,"message":"no","data":{"x":1}}}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":{"ok":true}}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":"again"}"#,
        r#"{"jsonrpc":"2.0","id":99,"result":null}"#,
        r#"{"jsonrpc":"2.0","id":"1","result":null}"#,
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad"}}"#,
    ] {
        assert!(e.handle(body.as_bytes()).is_ok());
    }
    assert!(sent(&rx).is_empty(), "responses are never answered");
    assert_eq!(
        a.wait().map(|r| r.as_str().to_owned()),
        Ok(r#"{"ok":true}"#.to_owned())
    );
    let rejected = b.wait();
    let Err(rpc_io::CallError::Rejected(error)) = rejected else {
        panic!("{rejected:?}")
    };
    assert_eq!((error.code(), error.message()), (ErrorCode(5), "no"));
    assert_eq!(error.data().map(|d| d.as_str()), Some(r#"{"x":1}"#));
    let events = events
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(
        events.iter().filter(|e| e.starts_with("Unmatched")).count(),
        3,
        "{events:?}"
    );
    assert_eq!(
        events.iter().filter(|e| e.starts_with("PeerError")).count(),
        1,
        "{events:?}"
    );
}

#[test]
fn test_malformed_response_ends_the_call_and_is_not_answered() {
    let (mut e, rx) = endpoint(Router::new(), ());
    let call = e.peer().request("a", ()).map_err(|e| e.to_string());
    let Ok(call) = call else {
        panic!("request failed")
    };
    let _request = sent(&rx);
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":1,"message":"m"}}"#)
            .is_ok()
    );
    assert!(sent(&rx).is_empty());
    assert!(matches!(
        call.wait(),
        Err(rpc_io::CallError::InvalidResponse(_))
    ));
}

// --- Closing and failures ----------------------------------------------------

#[test]
fn test_failing_sink_closes_the_endpoint() {
    let router = Router::new().request("m", |_: &mut (), _, _| Ok(1));
    let mut e = Endpoint::new(Faulty { panic: false }, router, ());
    assert!(matches!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"m"}"#),
        Err(Error::Io(_))
    ));
    assert!(e.is_closed());
    assert!(matches!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"m"}"#),
        Err(Error::Closed)
    ));
    assert!(matches!(e.peer().notify("x", ()), Err(Error::Closed)));
}

#[test]
fn test_close_ends_calls_and_cancels_deferred_work() {
    let (mut e, rx) = spec();
    assert!(exchange(&mut e, &rx, r#"{"jsonrpc":"2.0","id":1,"method":"later"}"#).is_empty());
    let call = e.peer().request("x", ()).map_err(|e| e.to_string());
    let Ok(call) = call else {
        panic!("request failed")
    };
    e.close();
    assert_eq!(call.wait(), Err(rpc_io::CallError::Closed));
    let held = e.state_mut().pop();
    let Some(held) = held else {
        panic!("no responder")
    };
    assert!(held.is_cancelled());
    assert!(matches!(held.ok(1), Err(Error::Closed)));
}

#[test]
fn test_stop_after_a_batch_answers_the_whole_batch() {
    let router = Router::new()
        .notification("exit", |_: &mut (), _, cx| cx.stop())
        .request("echo", |_, params, _| {
            Ok(params
                .into_raw()
                .map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
        });
    let (mut e, rx) = endpoint(router, ());
    let flow = e.handle(br#"[{"jsonrpc":"2.0","method":"exit"},{"jsonrpc":"2.0","id":1,"method":"echo","params":[1]}]"#);
    assert_eq!(flow.ok(), Some(Flow::Stop));
    assert_eq!(sent(&rx), [r#"[{"jsonrpc":"2.0","id":1,"result":[1]}]"#]);
}

#[test]
fn test_context_reports_method_and_id_and_sends_before_the_response() {
    let router = Router::new().request("work", |_: &mut (), _, cx| {
        cx.notify(
            "progress",
            json!({ "id": cx.id().and_then(|id| id.as_str()).unwrap_or("") }),
        )?;
        Ok(cx.method().to_owned())
    });
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":"k","method":"work"}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","method":"progress","params":{"id":"k"}}"#,
            r#"{"jsonrpc":"2.0","id":"k","result":"work"}"#
        ]
    );
}
