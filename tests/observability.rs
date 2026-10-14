//! What the endpoint reports about itself: the `Stats` counters. (The
//! `tracing` instrumentation is tested in `tests/tracing.rs`, alone in its
//! binary: `tracing` caches each call site's interest process-wide, so tests
//! running alongside without a subscriber would race with it.)
#![cfg(feature = "std")]

mod common;

use std::time::Duration;

use common::{endpoint, quiet_deliberate_panics, sent};
use rpc_io::{CallError, Config, Gate, Responder, Router, Stats};

fn router() -> Router<Vec<Responder>> {
    Router::<Vec<Responder>>::new()
        .request("ok", |_, _, _| Ok(1))
        .request("fail", |_, _, _| {
            Err::<(), _>(rpc_io::ErrorObject::internal_error("no"))
        })
        .request("boom", |_, _, _| -> Result<(), rpc_io::ErrorObject> {
            panic!("deliberate: stats")
        })
        .deferred("later", |held, _, _, responder| held.push(responder))
        .notification("n", |_, _, _| {})
}

#[test]
fn test_stats_count_traffic_both_ways() {
    quiet_deliberate_panics();
    let (mut e, rx) = endpoint(router(), Vec::new());
    assert_eq!(e.stats(), Stats::default());
    let bodies: [&[u8]; 6] = [
        br#"{"jsonrpc":"2.0","id":1,"method":"ok"}"#,
        br#"{"jsonrpc":"2.0","id":2,"method":"fail"}"#,
        br#"{"jsonrpc":"2.0","id":3,"method":"boom"}"#,
        br#"[{"jsonrpc":"2.0","method":"n"},{"jsonrpc":"2.0","id":4,"method":"later"},7]"#,
        br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":4}}"#,
        br#"{"jsonrpc":"2.0","id":99,"result":null}"#,
    ];
    let mut bytes = 0;
    for body in bodies {
        bytes += body.len() as u64;
        assert!(e.handle(body).is_ok());
    }
    let stats = e.stats();
    assert_eq!(stats.bodies_in, 6);
    assert_eq!(stats.bytes_in, bytes);
    assert_eq!(stats.requests_in, 4);
    assert_eq!(stats.notifications_in, 2);
    assert_eq!(stats.responses_in, 1);
    assert_eq!(stats.invalid_in, 1);
    assert_eq!(stats.panics, 1);
    assert_eq!(stats.cancelled_in, 1);
    // ok, fail, boom, then the batch (the cancelled `later` and the `7`).
    assert_eq!(stats.responses_out, 5);
    assert_eq!(stats.errors_out, 4);
    assert_eq!(stats.bodies_out, 4);
    let out: u64 = sent(&rx).iter().map(|m| m.len() as u64).sum();
    assert_eq!(stats.bytes_out, out);
    assert_eq!((stats.pending, stats.in_flight, stats.held), (0, 0, 0));
}

#[test]
fn test_stats_count_the_outcomes_of_our_requests() {
    let (e, _rx) = endpoint(Router::<()>::new(), ());
    let e = e.with_config(Config::new().with_max_pending(2).with_timer(false));
    let peer = e.peer();
    let first = peer.request("a", ());
    let second = peer.request("b", ());
    let third = peer.request_with_timeout("c", (), Duration::ZERO);
    let (Ok(first), Ok(second), Ok(third)) = (first, second, third) else {
        panic!("request failed")
    };
    assert_eq!(first.wait(), Err(CallError::Evicted));
    assert!(second.cancel());
    e.expire();
    assert_eq!(third.wait(), Err(CallError::TimedOut));
    let stats = peer.stats();
    assert_eq!(stats.requests_out, 3);
    assert_eq!(
        (stats.evicted, stats.cancelled_out, stats.timed_out),
        (1, 1, 1)
    );
    // Each ending told the peer: three cancel notifications.
    assert_eq!(stats.notifications_out, 3);
    assert_eq!(stats.pending, 0);
}

#[test]
fn test_stats_gauges_follow_what_is_outstanding() {
    let (e, _rx) = endpoint(router(), Vec::new());
    let mut e = e.with_config(Config::new().with_gate(Some(Gate::new("ok"))));
    let _call = e.peer().request("held", ());
    assert!(e.peer().notify("held", ()).is_ok());
    assert_eq!((e.stats().pending, e.stats().held), (1, 2));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"ok"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"later"}"#)
            .is_ok()
    );
    let stats = e.stats();
    assert_eq!((stats.pending, stats.held, stats.in_flight), (1, 0, 1));
    // The held request went out as a request, the notification as one.
    assert_eq!((stats.requests_out, stats.notifications_out), (1, 1));
}

#[test]
fn test_stats_count_a_skipped_message_as_invalid_not_as_a_body() {
    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut e = rpc_io::Endpoint::new(tx, Router::<()>::new(), ())
        .with_config(Config::new().with_limits(rpc_io::Limits::new().with_max_message_bytes(4)));
    let input: &[u8] = b"123456789\n";
    assert!(e.run(input, rpc_io::Framing::Newline).is_ok());
    let stats = e.stats();
    assert_eq!(
        (stats.bodies_in, stats.invalid_in, stats.errors_out),
        (0, 1, 1)
    );
}
