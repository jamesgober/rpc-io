//! Sessions over time: cancellation both ways, timeouts, eviction, the
//! opening-request gate, the blocking adapter, a client and a server over a
//! pipe, and races between answers and cancellations.
#![cfg(feature = "std")]

mod common;

use std::{
    collections::BTreeMap,
    sync::{Arc, Barrier, Mutex, PoisonError, mpsc},
    thread,
    time::Duration,
};

use common::{Shared, endpoint, pipe, sent};
use jsonx_lib::json;
use rpc_io::{
    CallError, Cancellation, Config, End, Endpoint, Error, ErrorCode, ErrorObject, Event,
    FrameError, Framed, Framing, Gate, Limits, Responder, Router,
};

fn later() -> Router<Vec<Responder>> {
    Router::<Vec<Responder>>::new().deferred("later", |held, _, _, responder| held.push(responder))
}

// --- The peer cancels our work ---------------------------------------------

#[test]
fn test_lsp_cancel_answers_once_with_request_cancelled() {
    let (mut e, rx) = endpoint(later(), Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":3,"method":"later"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":3}}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].starts_with(r#"{"jsonrpc":"2.0","id":3,"error":{"code":-32800,"#),
        "{}",
        replies[0]
    );
    let held = e.state_mut().pop();
    let Some(held) = held else {
        panic!("no responder")
    };
    assert!(held.is_cancelled());
    assert!(matches!(held.ok(1), Err(Error::AlreadyAnswered)));
    assert!(sent(&rx).is_empty());
    // Cancelling it again, or an id never seen, does nothing.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":3}}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":"nope"}}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":[3]}}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest"}"#)
            .is_ok()
    );
    assert!(sent(&rx).is_empty());
}

#[test]
fn test_mcp_cancel_sends_no_response() {
    let (e, rx) = endpoint(later(), Vec::new());
    let mut e = e.with_config(Config::new().with_cancellation(Some(Cancellation::mcp())));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":"r","method":"later"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"r","reason":"x"}}"#)
            .is_ok()
    );
    assert!(sent(&rx).is_empty());
    let held = e.state_mut().pop();
    assert!(
        held.is_some_and(|h| h.is_cancelled() && matches!(h.ok(1), Err(Error::AlreadyAnswered)))
    );
    assert!(sent(&rx).is_empty());
}

#[test]
fn test_cancel_inside_the_same_batch() {
    let (mut e, rx) = endpoint(later(), Vec::new());
    let body = br#"[{"jsonrpc":"2.0","id":1,"method":"later"},{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}]"#;
    assert!(e.handle(body).is_ok());
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert!(
        replies[0].starts_with(r#"[{"jsonrpc":"2.0","id":1,"error":{"code":-32800"#),
        "{}",
        replies[0]
    );
}

#[test]
fn test_silently_cancelled_batch_sends_nothing() {
    let (e, rx) = endpoint(later(), Vec::new());
    let mut e = e.with_config(Config::new().with_cancellation(Some(Cancellation::mcp())));
    let body = br#"[{"jsonrpc":"2.0","id":1,"method":"later"},{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}]"#;
    assert!(e.handle(body).is_ok());
    assert!(sent(&rx).is_empty());
}

#[test]
fn test_cancellation_off_routes_the_notification_to_handlers() {
    let router = Router::new().notification("$/cancelRequest", |seen: &mut u32, _, _| *seen += 1);
    let (e, _rx) = endpoint(router, 0);
    let mut e = e.with_config(Config::new().with_cancellation(None));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#)
            .is_ok()
    );
    assert_eq!(*e.state(), 1);
}

// --- We cancel, time out, or evict our own requests --------------------------

#[test]
fn test_call_cancel_sends_the_cancel_notification() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let call = e.peer().request("slow", ()).map_err(|e| e.to_string());
    let Ok(call) = call else {
        panic!("request failed")
    };
    let canceller = call.canceller();
    assert!(canceller.cancel());
    assert!(!canceller.cancel());
    assert_eq!(call.wait(), Err(CallError::Cancelled));
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#,
            r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#
        ]
    );
}

#[test]
fn test_mcp_cancel_is_spelled_with_request_id() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_cancellation(Some(Cancellation::mcp())));
    let call = e.peer().request("slow", ()).map_err(|e| e.to_string());
    assert!(call.is_ok_and(|c| c.cancel()));
    assert_eq!(
        sent(&rx)[1],
        r#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#
    );
    // Off: nothing is sent.
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_cancellation(None));
    let call = e.peer().request("slow", ()).map_err(|e| e.to_string());
    assert!(call.is_ok_and(|c| c.cancel()));
    assert_eq!(sent(&rx).len(), 1);
}

#[test]
fn test_timeout_ends_the_call_and_tells_the_peer() {
    let (mut e, rx) = endpoint(Router::<()>::new(), ());
    let call = e
        .peer()
        .request_with_timeout("slow", (), Duration::from_millis(20));
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert!(call.deadline().is_some());
    assert_eq!(call.wait(), Err(CallError::TimedOut));
    let replies = sent(&rx);
    assert_eq!(
        replies[1],
        r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#
    );
    // The late answer is unmatched and changes nothing.
    assert!(e.handle(br#"{"jsonrpc":"2.0","id":1,"result":1}"#).is_ok());
}

#[test]
fn test_timeout_fires_with_no_traffic_and_no_waiter() {
    // Found by the audit (RPC-COR-03): a deadline was enforced only when a
    // message arrived or someone waited, so a fire-and-forget request never
    // told the peer to cancel while the connection was quiet.
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let call = e
        .peer()
        .request_with_timeout("slow", (), Duration::from_millis(30));
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).ok().as_deref(),
        Some(r#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#)
    );
    // Nobody calls `expire`, `wait`, or `handle`: the timer does it.
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(5)).ok().as_deref(),
        Some(r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#)
    );
    assert!(e.next_deadline().is_none());
    assert!(matches!(call.try_wait(), Ok(Err(CallError::TimedOut))));
}

#[test]
fn test_timer_fires_deadlines_in_order_and_rearms() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let peer = e.peer();
    let late = peer.request_with_timeout("late", (), Duration::from_millis(80));
    let early = peer.request_with_timeout("early", (), Duration::from_millis(20));
    let (Ok(late), Ok(early)) = (late, early) else {
        panic!("request failed")
    };
    let mut cancels = Vec::new();
    while cancels.len() < 2 {
        let Ok(message) = rx.recv_timeout(Duration::from_secs(5)) else {
            panic!("a deadline did not fire")
        };
        if message.contains("cancelRequest") {
            cancels.push(message);
        }
    }
    assert!(cancels[0].contains(r#""id":2"#), "{cancels:?}");
    assert!(cancels[1].contains(r#""id":1"#), "{cancels:?}");
    assert!(matches!(early.try_wait(), Ok(Err(CallError::TimedOut))));
    assert!(matches!(late.try_wait(), Ok(Err(CallError::TimedOut))));
}

#[test]
fn test_timer_off_leaves_deadlines_to_the_caller() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_timer(false));
    let call = e
        .peer()
        .request_with_timeout("slow", (), Duration::from_millis(10));
    let Ok(call) = call else {
        panic!("request failed")
    };
    thread::sleep(Duration::from_millis(100));
    assert_eq!(sent(&rx).len(), 1, "only the request went out");
    e.expire();
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#]
    );
    assert!(matches!(call.try_wait(), Ok(Err(CallError::TimedOut))));
}

#[test]
fn test_timer_does_not_keep_the_connection_alive() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let call = e
        .peer()
        .request_with_timeout("slow", (), Duration::from_secs(3600));
    let Ok(call) = call else {
        panic!("request failed")
    };
    drop(e);
    // Closed at once, not after an hour; and the sink (the channel) is gone.
    assert_eq!(call.wait(), Err(CallError::Closed));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while rx.try_recv().is_ok() || !matches!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected)) {
        assert!(
            std::time::Instant::now() < deadline,
            "the sink was kept alive"
        );
        thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn test_config_timeout_applies_to_every_request() {
    let (e, _rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_request_timeout(Some(Duration::from_millis(5))));
    let call = e.peer().request("slow", ());
    assert!(call.is_ok_and(|c| c.wait() == Err(CallError::TimedOut)));
}

#[test]
fn test_eviction_at_max_pending() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_max_pending(2));
    let peer = e.peer();
    let calls: Vec<_> = (0..3).filter_map(|_| peer.request("x", ()).ok()).collect();
    assert_eq!(calls.len(), 3);
    let mut calls = calls.into_iter();
    assert_eq!(
        calls.next().map(|c| c.wait()),
        Some(Err(CallError::Evicted))
    );
    let replies = sent(&rx);
    assert!(
        replies.contains(
            &r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#.to_owned()
        )
    );
}

#[test]
fn test_waiting_inside_a_handler_would_deadlock() {
    let router = Router::new().request("ask", |_: &mut (), _, cx| {
        let call = cx.request("question", ())?;
        Ok(format!("{:?}", call.wait()))
    });
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"ask"}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    // `wait` consumed the call, so the call is dropped and cancelled before the
    // handler returns.
    assert_eq!(
        replies[1],
        r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#
    );
    assert_eq!(
        replies[2],
        r#"{"jsonrpc":"2.0","id":1,"result":"Err(WouldDeadlock)"}"#
    );
}

#[test]
fn test_a_dropped_call_frees_its_slot_and_a_detached_one_does_not() {
    let (e, rx) = endpoint(Router::new(), ());
    let mut e = e.with_config(Config::new().with_max_pending(1));
    for _ in 0..10 {
        drop(e.peer().request("abandoned", ()).ok());
    }
    assert_eq!(e.stats().pending, 0);
    assert_eq!(e.stats().evicted, 0);
    let replies = sent(&rx);
    assert_eq!(replies.len(), 20, "a request and a cancel for each");
    assert!(replies[1].contains("$/cancelRequest"), "{}", replies[1]);
    // A detached call stays outstanding and sends no cancel.
    if let Ok(call) = e.peer().request("kept", ()) {
        call.detach();
    }
    assert_eq!(e.stats().pending, 1);
    assert_eq!(sent(&rx).len(), 1, "only the request");
    // An answered call has nothing left to cancel when dropped.
    assert!(e.handle(br#"{"jsonrpc":"2.0","id":11,"result":1}"#).is_ok());
    assert_eq!(e.stats().pending, 0);
    let answered = e.peer().request("answered", ());
    assert!(e.handle(br#"{"jsonrpc":"2.0","id":12,"result":2}"#).is_ok());
    drop(answered);
    assert_eq!(sent(&rx).len(), 1, "only the request, no cancel");
}

#[test]
fn test_outgoing_params_must_be_structured() {
    let (e, rx) = endpoint(Router::<()>::new(), ());
    let peer = e.peer();
    assert!(matches!(
        peer.notify("x", 1),
        Err(Error::InvalidParams(jsonx_lib::Kind::Number))
    ));
    assert!(matches!(
        peer.request("x", "s"),
        Err(Error::InvalidParams(jsonx_lib::Kind::String))
    ));
    assert!(peer.notify("x", rpc_io::Raw::NULL).is_ok());
    assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","method":"x"}"#]);
}

// --- The gate ------------------------------------------------------------------

fn gated() -> (
    Endpoint<Vec<String>, mpsc::Sender<String>>,
    mpsc::Receiver<String>,
) {
    let router = Router::<Vec<String>>::new()
        .request("initialize", |_, params, cx| {
            // Sent during the opening request: held until it succeeds.
            cx.notify("window/logMessage", json!({ "message": "hello" }))?;
            match params.parse()?["fail"].as_bool() {
                Some(true) => Err(ErrorObject::new(ErrorCode(1), "refused")),
                _ => Ok(json!({ "capabilities": {} })),
            }
        })
        .request("hover", |_, _, _| Ok("hovered"))
        .notification("exit", |log, _, _| log.push("exit".into()))
        .notification("initialized", |log, _, _| log.push("initialized".into()))
        .on_event(|log, event| {
            if let Event::Dropped { method } = event {
                log.push(format!("dropped {method}"));
            }
        });
    let (e, rx) = endpoint(router, Vec::new());
    let e = e.with_config(Config::new().with_gate(Some(Gate::new("initialize").allow("exit"))));
    (e, rx)
}

#[test]
fn test_gate_refuses_and_drops_until_the_opening_request_succeeds() {
    let (mut e, rx) = gated();
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"hover"}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert!(
        replies[0].starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32002,"#),
        "{replies:?}"
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"initialized"}"#)
            .is_ok()
    );
    assert!(e.handle(br#"{"jsonrpc":"2.0","method":"exit"}"#).is_ok());
    assert_eq!(e.state(), &["dropped initialized", "exit"]);
    // Outgoing traffic is held, then sent right after the opening response.
    assert!(e.peer().notify("early", ()).is_ok());
    assert!(sent(&rx).is_empty());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [
            r#"{"jsonrpc":"2.0","id":2,"result":{"capabilities":{}}}"#,
            r#"{"jsonrpc":"2.0","method":"early"}"#,
            r#"{"jsonrpc":"2.0","method":"window/logMessage","params":{"message":"hello"}}"#,
        ]
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":3,"method":"hover"}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","id":3,"result":"hovered"}"#]
    );
    // A second opening request is refused.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":4,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].starts_with(r#"{"jsonrpc":"2.0","id":4,"error":{"code":-32600,"#));
}

#[test]
fn test_failed_opening_request_discards_held_traffic_and_stays_shut() {
    let (mut e, rx) = gated();
    let held = e.peer().request("workspace/configuration", ());
    let Ok(held) = held else {
        panic!("request failed")
    };
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"fail":true}}"#)
            .is_ok()
    );
    assert_eq!(
        sent(&rx),
        [r#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"refused"}}"#]
    );
    assert_eq!(held.wait(), Err(CallError::Cancelled));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"hover"}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].contains("-32002"));
    // It can be tried again.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":3,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].starts_with(r#"{"jsonrpc":"2.0","id":3,"result""#));
}

#[test]
fn test_messages_held_behind_the_gate_are_bounded() {
    // RPC-SEC-05: what this side sends before the gate opens is held in
    // memory; the budget bounds it.
    let router = Router::new().request("initialize", |_: &mut (), _, _| Ok(json!({})));
    let (e, rx) = endpoint(router, ());
    let mut e = e.with_config(
        Config::new()
            .with_gate(Some(Gate::new("initialize")))
            .with_max_held_bytes(100),
    );
    let peer = e.peer();
    // `{"jsonrpc":"2.0","method":"held"}` is 33 bytes: three fit, the fourth does not.
    let results: Vec<_> = (0..4).map(|_| peer.notify("held", ())).collect();
    assert!(results[..3].iter().all(Result::is_ok));
    assert!(matches!(results[3], Err(Error::Backlog)));
    assert!(matches!(peer.request("ask", ()), Err(Error::Backlog)));
    assert!(!peer.is_closed());
    assert_eq!(
        Error::Backlog.to_string(),
        "too much is already held until the opening request succeeds; nothing was sent"
    );
    // The opening request goes through; the held messages follow it, and the
    // budget is free again.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert_eq!(replies.len(), 4, "{replies:?}"); // the response, then the 3 held
    assert_eq!(replies.iter().filter(|r| r.contains("held")).count(), 3);
    assert!(peer.notify("after", ()).is_ok());
}

#[test]
fn test_opening_request_in_a_batch_is_refused() {
    let (mut e, rx) = gated();
    assert!(
        e.handle(br#"[{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}]"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert!(
        replies[0].starts_with(r#"[{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"#),
        "{replies:?}"
    );
}

// --- The blocking adapter ------------------------------------------------------

fn echo() -> Router<()> {
    Router::new()
        .request("echo", |_, params, _| {
            Ok(params
                .into_raw()
                .map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
        })
        .notification("exit", |_, _, cx| cx.stop())
}

#[test]
fn test_run_content_length_end_to_end() {
    let out = Shared::default();
    let mut e = Endpoint::new(Framed::new(out.clone(), Framing::ContentLength), echo(), ());
    let mut input = Vec::new();
    for body in [
        r#"{"jsonrpc":"2.0","id":1,"method":"echo","params":[1]}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"echo","params":{"a":"\n"}}"#,
    ] {
        Framing::ContentLength.encode(body.as_bytes(), &mut input);
    }
    assert_eq!(
        e.run(input.as_slice(), Framing::ContentLength).ok(),
        Some(End::Eof)
    );
    assert_eq!(
        out.text(),
        "Content-Length: 37\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[1]}Content-Length: 44\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"a\":\"\\n\"}}"
    );
    assert!(e.is_closed());
}

#[test]
fn test_run_answers_oversized_messages_and_carries_on() {
    let out = Shared::default();
    // Recorded, not asserted in the hook: a panic there is contained.
    let events = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&events);
    let router = echo().on_event(move |_, event| {
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(format!("{event:?}"));
    });
    let mut e = Endpoint::new(Framed::new(out.clone(), Framing::Newline), router, ())
        .with_config(Config::new().with_limits(Limits::new().with_max_message_bytes(64)));
    let input = format!(
        "{}\n{}\n",
        "x".repeat(10_000),
        r#"{"jsonrpc":"2.0","id":1,"method":"echo"}"#
    );
    assert_eq!(
        e.run(input.as_bytes(), Framing::Newline).ok(),
        Some(End::Eof)
    );
    let text = out.text();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert!(lines[0].starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"#));
    assert_eq!(lines[1], r#"{"jsonrpc":"2.0","id":1,"result":null}"#);
    let events = events
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    assert_eq!(events.len(), 1, "{events:?}");
    assert!(events[0].starts_with("Oversized"), "{events:?}");
}

#[test]
fn test_run_stops_on_a_fatal_frame_error_and_closes() {
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, echo(), ());
    let call = e.peer().request("x", ());
    let Ok(call) = call else {
        panic!("request failed")
    };
    let result = e.run(&b"Content-Length: nope\r\n\r\n"[..], Framing::ContentLength);
    assert!(matches!(
        result,
        Err(Error::Frame(FrameError::InvalidContentLength))
    ));
    assert!(e.is_closed());
    assert_eq!(call.wait(), Err(CallError::Closed));
    assert!(matches!(
        e.run(&b""[..], Framing::Newline),
        Err(Error::Closed)
    ));
}

#[test]
fn test_run_truncated_stream_is_an_error() {
    let result = rpc_io::serve(
        &b"Content-Length: 10\r\n\r\n{}"[..],
        std::io::sink(),
        Framing::ContentLength,
        echo(),
    );
    assert!(matches!(result, Err(Error::Frame(FrameError::Truncated))));
}

#[test]
fn test_serve_stops_and_keeps_the_endpoint_open_on_stop() {
    let input = b"{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"echo\"}\n";
    let out = Shared::default();
    let mut e = Endpoint::new(Framed::new(out.clone(), Framing::Newline), echo(), ());
    assert_eq!(e.run(&input[..], Framing::Newline).ok(), Some(End::Stopped));
    assert!(!e.is_closed());
    assert!(
        out.text().is_empty(),
        "the message after `exit` was handled"
    );
    assert!(e.peer().notify("bye", ()).is_ok());
    assert_eq!(out.text(), "{\"jsonrpc\":\"2.0\",\"method\":\"bye\"}\n");
}

#[test]
fn test_run_resumed_after_stop_handles_the_read_ahead() {
    // Found by the audit (RPC-COR-01): the decoder reads ahead, and a second
    // `run` after a handler stopped used to start from an empty decoder,
    // losing every message read past the stop.
    let router = Router::new()
        .notification("pause", |_: &mut u32, _, cx| cx.stop())
        .notification("count", |n, _, _| *n += 1);
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, 0u32);
    let input = b"{\"jsonrpc\":\"2.0\",\"method\":\"pause\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"count\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"count\"}\n";
    let mut reader = &input[..];
    assert_eq!(
        e.run(&mut reader, Framing::Newline).ok(),
        Some(End::Stopped)
    );
    assert!(reader.is_empty(), "the reader was read ahead");
    assert_eq!(*e.state(), 0);
    assert_eq!(e.run(&mut reader, Framing::Newline).ok(), Some(End::Eof));
    assert_eq!(*e.state(), 2);
}

#[test]
fn test_read_ahead_carries_over_to_another_framing() {
    let router = Router::new()
        .notification("upgrade", |_: &mut Vec<String>, _, cx| cx.stop())
        .notification("hello", |log, params, _| {
            log.push(
                params
                    .raw()
                    .map(|r| r.as_str().to_owned())
                    .unwrap_or_default(),
            );
        });
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, Vec::new());
    let mut input = b"{\"jsonrpc\":\"2.0\",\"method\":\"upgrade\"}\n".to_vec();
    Framing::ContentLength.encode(
        br#"{"jsonrpc":"2.0","method":"hello","params":[1]}"#,
        &mut input,
    );
    let mut reader = input.as_slice();
    assert_eq!(
        e.run(&mut reader, Framing::Newline).ok(),
        Some(End::Stopped)
    );
    assert_eq!(
        e.run(&mut reader, Framing::ContentLength).ok(),
        Some(End::Eof)
    );
    assert_eq!(e.state(), &["[1]"]);
}

#[test]
fn test_next_read_commit_and_finish_drive_the_endpoint() {
    use rpc_io::Next;
    let router = Router::new()
        .request("echo", |_: &mut (), params, _| {
            Ok(params
                .into_raw()
                .map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
        })
        .notification("exit", |_, _, cx| cx.stop());
    let out = Shared::default();
    let mut e = Endpoint::new(Framed::new(out.clone(), Framing::Newline), router, ());
    // A last line without its `\n`, delivered three bytes at a time.
    let wire = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"echo\",\"params\":[1]}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"echo\"}";
    for chunk in wire.chunks(3) {
        match e.next_read(Framing::Newline) {
            Ok(Next::Read(buf)) => buf[..chunk.len()].copy_from_slice(chunk),
            other => panic!("{other:?}"),
        }
        e.commit(chunk.len());
    }
    assert_eq!(e.finish().ok(), Some(End::Eof));
    assert_eq!(
        out.text(),
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":[1]}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":null}\n"
    );
    assert!(e.is_closed());
    assert!(matches!(e.next_read(Framing::Newline), Err(Error::Closed)));
    assert!(matches!(e.finish(), Err(Error::Closed)));
    e.commit(5); // nothing to commit to: ignored
}

#[test]
fn test_next_read_reports_a_stop_and_keeps_what_follows() {
    use rpc_io::Next;
    let router = Router::new()
        .notification("exit", |_: &mut u32, _, cx| cx.stop())
        .notification("n", |n, _, _| *n += 1);
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, 0u32);
    let wire =
        b"{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n";
    if let Ok(Next::Read(buf)) = e.next_read(Framing::Newline) {
        buf[..wire.len()].copy_from_slice(wire);
    }
    e.commit(wire.len());
    assert!(matches!(e.next_read(Framing::Newline), Ok(Next::Stopped)));
    assert_eq!(*e.state(), 0);
    assert!(matches!(e.next_read(Framing::Newline), Ok(Next::Read(_))));
    assert_eq!(*e.state(), 1);
    assert!(!e.is_closed());
}

#[test]
fn test_next_read_closes_on_a_fatal_frame_error() {
    use rpc_io::Next;
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, Router::<()>::new(), ());
    let wire = b"Content-Length: x\r\n\r\n";
    if let Ok(Next::Read(buf)) = e.next_read(Framing::ContentLength) {
        buf[..wire.len()].copy_from_slice(wire);
    }
    e.commit(wire.len());
    assert!(matches!(
        e.next_read(Framing::ContentLength),
        Err(Error::Frame(FrameError::InvalidContentLength))
    ));
    assert!(e.is_closed());
}

#[test]
fn test_finish_inside_a_message_is_truncated() {
    use rpc_io::Next;
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, Router::<()>::new(), ());
    let wire = b"Content-Length: 10\r\n\r\n{}";
    if let Ok(Next::Read(buf)) = e.next_read(Framing::ContentLength) {
        buf[..wire.len()].copy_from_slice(wire);
    }
    e.commit(wire.len());
    assert!(matches!(
        e.finish(),
        Err(Error::Frame(FrameError::Truncated))
    ));
    assert!(e.is_closed());
}

#[test]
fn test_run_with_a_caller_decoder_leaves_the_endpoint_decoder_alone() {
    let router = Router::new().notification("stop", |_: &mut (), _, cx| cx.stop());
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, ());
    let mut decoder = Framing::Newline.decoder(&Limits::new());
    let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"method\":\"stop\"}\nrest";
    assert_eq!(e.run_with(&mut decoder, input).ok(), Some(End::Stopped));
    assert_eq!(decoder.buffered(), 4);
    // A limits change keeps the endpoint's own (empty) state consistent.
    let mut e = e.with_config(Config::new().with_limits(Limits::new().with_max_batch(2)));
    assert_eq!(e.run(&b""[..], Framing::Newline).ok(), Some(End::Eof));
    let mut decoder = Framing::Newline.decoder(&Limits::new());
    assert!(matches!(
        e.run_with(&mut decoder, &b""[..]),
        Err(Error::Closed)
    ));
}

#[test]
fn test_config_change_after_a_stop_keeps_the_read_ahead() {
    let router = Router::new()
        .notification("stop", |_: &mut u32, _, cx| cx.stop())
        .notification("n", |n, _, _| *n += 1);
    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, 0u32);
    let input =
        b"{\"jsonrpc\":\"2.0\",\"method\":\"stop\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"n\"}\n";
    let mut reader = &input[..];
    assert_eq!(
        e.run(&mut reader, Framing::Newline).ok(),
        Some(End::Stopped)
    );
    let mut e = e.with_config(Config::new());
    assert_eq!(e.run(&mut reader, Framing::Newline).ok(), Some(End::Eof));
    assert_eq!(*e.state(), 1);
}

// --- A client and a server over a pipe -----------------------------------------

#[test]
fn test_client_and_server_over_a_pipe_correlate_concurrent_requests() {
    let (to_server, server_in) = pipe();
    let (to_client, client_in) = pipe();
    let server = thread::spawn(move || {
        let router = Router::new()
            .request("add", |_: &mut (), params, _| {
                let p = params.parse()?;
                Ok(p[0].as_i64().unwrap_or(0) + p[1].as_i64().unwrap_or(0))
            })
            .notification("exit", |_, _, cx| cx.stop());
        // Returning drops the server's endpoint and with it its output, which
        // ends the client's input.
        rpc_io::serve(server_in, to_client, Framing::ContentLength, router)
    });
    let mut client = Endpoint::new(
        Framed::new(to_server, Framing::ContentLength),
        Router::<()>::new(),
        (),
    );
    let peer = client.peer();
    let reader = thread::spawn(move || client.run(client_in, Framing::ContentLength));
    let workers: Vec<_> = (0..4i64)
        .map(|w| {
            let peer = peer.clone();
            thread::spawn(move || {
                (0..250i64).all(|i| {
                    let call = peer.request("add", json!([w * 1000, i]));
                    call.is_ok_and(|c| {
                        c.wait()
                            .is_ok_and(|r| r.as_str() == (w * 1000 + i).to_string())
                    })
                })
            })
        })
        .collect();
    for worker in workers {
        assert!(worker.join().is_ok_and(|all_right| all_right));
    }
    assert!(peer.notify("exit", ()).is_ok());
    assert!(matches!(server.join().ok(), Some(Ok(End::Stopped))));
    assert!(matches!(reader.join().ok(), Some(Ok(End::Eof))));
    assert!(peer.is_closed());
}

// --- Races -----------------------------------------------------------------------

#[test]
fn test_race_between_a_worker_answer_and_a_peer_cancel_gives_one_response() {
    const N: i64 = 300;
    let router = Router::<()>::new().deferred("work", |_, _, _, responder| {
        thread::spawn(move || responder.ok("done"));
    });
    let (mut e, rx) = endpoint(router, ());
    for id in 0..N {
        let request = format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"work"}}"#);
        let cancel =
            format!(r#"{{"jsonrpc":"2.0","method":"$/cancelRequest","params":{{"id":{id}}}}}"#);
        assert!(e.handle(request.as_bytes()).is_ok());
        assert!(e.handle(cancel.as_bytes()).is_ok());
    }
    // Let every worker finish.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut per_id: BTreeMap<i64, u32> = BTreeMap::new();
    while per_id.len() < N as usize && std::time::Instant::now() < deadline {
        if let Ok(reply) = rx.recv_timeout(Duration::from_millis(50)) {
            let doc = jsonx_lib::parse(&reply).map(|v| v["id"].as_i64());
            if let Ok(Some(id)) = doc {
                *per_id.entry(id).or_default() += 1;
            }
        }
    }
    thread::sleep(Duration::from_millis(100));
    for reply in rx.try_iter() {
        if let Ok(Some(id)) = jsonx_lib::parse(&reply).map(|v| v["id"].as_i64()) {
            *per_id.entry(id).or_default() += 1;
        }
    }
    assert_eq!(per_id.len(), N as usize);
    assert!(per_id.values().all(|&n| n == 1), "{per_id:?}");
}

#[test]
fn test_race_between_an_answer_and_our_cancel_gives_one_outcome() {
    for _ in 0..200 {
        let unmatched = Arc::new(Mutex::new(0u32));
        let count = Arc::clone(&unmatched);
        let router = Router::new().on_event(move |_: &mut (), event| {
            if matches!(event, Event::Unmatched { .. }) {
                *count.lock().unwrap_or_else(PoisonError::into_inner) += 1;
            }
        });
        let (mut e, _rx) = endpoint(router, ());
        let call = e.peer().request("x", ());
        let Ok(call) = call else {
            panic!("request failed")
        };
        let canceller = call.canceller();
        let start = Arc::new(Barrier::new(2));
        let go = Arc::clone(&start);
        let cancelling = thread::spawn(move || {
            go.wait();
            canceller.cancel()
        });
        start.wait();
        assert!(e.handle(br#"{"jsonrpc":"2.0","id":1,"result":7}"#).is_ok());
        let cancelled = cancelling.join().unwrap_or(false);
        let outcome = call.wait();
        let unmatched = *unmatched.lock().unwrap_or_else(PoisonError::into_inner);
        if cancelled {
            assert_eq!(outcome, Err(CallError::Cancelled));
            assert_eq!(unmatched, 1);
        } else {
            assert_eq!(outcome.map(|r| r.as_str().to_owned()), Ok("7".to_owned()));
            assert_eq!(unmatched, 0);
        }
    }
}

#[test]
fn test_held_request_that_times_out_is_never_sent() {
    let (mut e, rx) = gated();
    let call = e.peer().request_with_timeout("early", (), Duration::ZERO);
    let Ok(call) = call else {
        panic!("request failed")
    };
    e.expire();
    assert_eq!(call.wait(), Err(CallError::TimedOut));
    // No cancel notification for a request the peer never saw, and no
    // request either once the gate opens.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert!(
        replies
            .iter()
            .all(|r| !r.contains("early") && !r.contains("cancelRequest")),
        "{replies:?}"
    );
}
