//! Tripwires for quadratic paths: each would take minutes if the work it
//! drives were quadratic, and takes well under a second when linear.
#![cfg(feature = "std")]

mod common;

use common::{endpoint, sent};
use rpc_io::{Config, Endpoint, Framing, Gate, Limits, Responder, Router};

#[test]
fn test_scale_large_body_fed_a_byte_at_a_time() {
    const N: usize = 4 << 20;
    let mut input = format!("Content-Length: {N}\r\n\r\n").into_bytes();
    input.resize(input.len() + N, b' ');
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    let mut got = 0;
    for byte in input.chunks(1) {
        let _taken = decoder.feed(byte);
        while let Ok(Some(frame)) = decoder.next_frame() {
            got += frame.len();
        }
    }
    assert_eq!(got, N);
}

#[test]
fn test_scale_long_line_fed_a_byte_at_a_time() {
    const N: usize = 4 << 20;
    let mut input = vec![b' '; N];
    input.push(b'\n');
    let mut decoder = Framing::Newline.decoder(&Limits::new());
    for byte in input.chunks(1) {
        let _taken = decoder.feed(byte);
        while let Ok(Some(_)) = decoder.next_frame() {}
    }
    assert_eq!(decoder.buffered(), 0);
}

#[test]
fn test_scale_full_batch_of_deferred_requests_cancelled() {
    let router = Router::<Vec<Responder>>::new().deferred("later", |held, _, _, r| held.push(r));
    let (mut e, rx) = endpoint(router, Vec::new());
    let max = Limits::new().max_batch;
    let mut batch = String::from("[");
    for i in 0..max {
        if i > 0 {
            batch.push(',');
        }
        batch.push_str(&format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"later"}}"#));
    }
    batch.push(']');
    assert!(e.handle(batch.as_bytes()).is_ok());
    let mut cancels = String::from("[");
    for i in 0..max {
        if i > 0 {
            cancels.push(',');
        }
        cancels.push_str(&format!(
            r#"{{"jsonrpc":"2.0","method":"$/cancelRequest","params":{{"id":{i}}}}}"#
        ));
    }
    cancels.push(']');
    assert!(e.handle(cancels.as_bytes()).is_ok());
    let replies = sent(&rx);
    assert_eq!(replies.len(), 1);
    assert_eq!(replies[0].matches("-32800").count(), max);
}

#[test]
fn test_scale_many_messages_through_one_endpoint() {
    let router = Router::new().request("inc", |n: &mut u64, _, _| {
        *n += 1;
        Ok(*n)
    });
    let (mut e, rx) = endpoint(router, 0u64);
    for i in 0..100_000u64 {
        let body = format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"inc"}}"#);
        assert!(e.handle(body.as_bytes()).is_ok());
    }
    assert_eq!(*e.state(), 100_000);
    assert_eq!(rx.try_iter().count(), 100_000);
}

#[test]
fn test_scale_peer_responses_while_much_is_held() {
    // Before the gate opens, our side holds many requests; the peer floods
    // responses. Each lookup must not scan what is held.
    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let e = Endpoint::new(tx, Router::<()>::new(), ()).with_config(
        Config::new()
            .with_gate(Some(Gate::new("initialize")))
            .with_max_pending(50_000),
    );
    let peer = e.peer();
    let calls: Vec<_> = (0..50_000)
        .filter_map(|_| peer.request("x", ()).ok())
        .collect();
    assert_eq!(calls.len(), 50_000);
    let mut e = e;
    for i in 1..=50_000u64 {
        let body = format!(r#"{{"jsonrpc":"2.0","id":{i},"result":1}}"#);
        assert!(e.handle(body.as_bytes()).is_ok());
    }
    // Every forged answer was refused: all calls are still outstanding.
    assert!(calls.into_iter().all(|c| c.try_wait().is_err()));
}
