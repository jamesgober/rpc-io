//! Regression tests for the findings of the adversarial review. Each was
//! written to fail before its fix.
#![cfg(feature = "std")]

mod common;

use std::{
    panic::{self, AssertUnwindSafe},
    sync::{Arc, Mutex},
};

use common::{Faulty, Shared, endpoint, sent};
use rpc_io::{
    Cancellation, Config, Endpoint, Error, Event, FrameError, Framed, Framing, Gate, Limits,
    Responder, Router,
};

#[test]
fn test_header_name_with_non_token_bytes_is_invalid() {
    // A sender that forgot the blank line: the body `{}` runs into the next
    // header. Read as a header named `{}Content-Length`, it would silently
    // swallow a message and resynchronise on the wrong boundary.
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    let _taken = decoder.feed(b"Content-Length: 2\r\n{}Content-Length: 2\r\n\r\n{}");
    assert_eq!(decoder.next_frame(), Err(FrameError::InvalidHeader));
}

#[test]
fn test_header_name_with_leading_space_is_invalid() {
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    let _taken = decoder.feed(b"Content-Length: 2\r\n Content-Type: x\r\n\r\n{}");
    assert_eq!(decoder.next_frame(), Err(FrameError::InvalidHeader));
}

#[test]
fn test_silent_cancel_of_opening_request_reopens_the_gate() {
    let router = Router::new().deferred(
        "initialize",
        |held: &mut Vec<Responder>, _, _, responder| {
            held.push(responder);
        },
    );
    let config = Config::new()
        .with_gate(Some(Gate::new("initialize")))
        .with_cancellation(Some(Cancellation::mcp()));
    let (endpoint, rx) = endpoint(router, Vec::new());
    let mut endpoint = endpoint.with_config(config);
    endpoint
        .handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)
        .ok();
    endpoint
        .handle(br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#)
        .ok();
    // MCP: no response to the cancelled request.
    assert!(sent(&rx).is_empty());
    // The opening request failed, so a new one is served, not refused.
    endpoint
        .handle(br#"{"jsonrpc":"2.0","id":2,"method":"initialize"}"#)
        .ok();
    assert_eq!(endpoint.state().len(), 2);
    assert!(sent(&rx).is_empty());
}

#[test]
fn test_sink_panic_is_contained_as_an_io_error() {
    let router = Router::new().request("m", |_, _, _| Ok(1));
    let mut endpoint = Endpoint::new(Faulty { panic: true }, router, ());
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
        endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"m"}"#)
    }));
    let Ok(result) = outcome else {
        panic!("a panicking sink unwound through the endpoint");
    };
    assert!(matches!(result, Err(Error::Io(_))), "{result:?}");
    assert!(endpoint.is_closed());
}

#[test]
fn test_send_from_another_thread_during_a_turn_is_flushed_at_once() {
    let out = Shared::default();
    let watched = out.clone();
    let router = Router::new().request("work", move |seen: &mut bool, _, cx| {
        let peer = cx.peer();
        std::thread::spawn(move || peer.notify("progress", ()))
            .join()
            .map_err(|_| rpc_io::ErrorObject::internal_error("worker"))??;
        // The worker's message is on the wire while this handler still runs.
        *seen = watched.text().contains("progress");
        Ok(())
    });
    let mut endpoint = Endpoint::new(Framed::new(out.clone(), Framing::Newline), router, false);
    endpoint
        .handle(br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#)
        .ok();
    assert!(*endpoint.state());
}

#[test]
fn test_answer_to_a_request_never_sent_is_unmatched() {
    let unmatched = Arc::new(Mutex::new(0));
    let count = Arc::clone(&unmatched);
    let router = Router::new().on_event(move |_: &mut (), event| {
        if matches!(event, Event::Unmatched { .. }) {
            *count
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        }
    });
    let (endpoint, rx) = endpoint(router, ());
    let mut endpoint = endpoint.with_config(Config::new().with_gate(Some(Gate::new("initialize"))));
    // Held: the gate is shut, so the request has not been sent.
    let call = endpoint
        .peer()
        .request("ask", ())
        .map_err(|e| e.to_string());
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert!(sent(&rx).is_empty());
    // A "response" to it can only be forged or confused.
    endpoint
        .handle(br#"{"jsonrpc":"2.0","id":1,"result":true}"#)
        .ok();
    assert!(
        call.try_wait().is_err(),
        "a forged answer settled an unsent request"
    );
    assert_eq!(
        *unmatched
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        1
    );
}

#[test]
fn test_repeated_id_after_another_repeated_member_answers_under_null() {
    // Found by the classification property: only the first repeated member
    // was remembered, so a repeated `id` behind a repeated `method` was
    // trusted, and the error went out under one of two conflicting ids.
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"m","method":"m","id":2}"#;
    let rpc_io::Incoming::Single(Err(invalid)) = rpc_io::decode(body, &Limits::new()) else {
        panic!("decoded as a message");
    };
    assert!(invalid.id().is_none(), "{:?}", invalid.id());
}

#[test]
fn test_oversized_blank_line_is_too_large_however_the_input_is_split() {
    // Found by the chunking property on Linux (seed kept in
    // `proptest-regressions/framing.txt`): a whitespace-only line over the
    // limit was skipped when it arrived whole, but reported as too large when
    // it arrived in pieces, because the length was checked after the
    // blankness on one path and before it on the other. The length now comes
    // first on both, and newline framing always reports `length: None`.
    let mut input = vec![b'\n'; 33];
    input.extend_from_slice(&[b'\r'; 42]);
    input.push(b'\n');
    let limits = Limits::new()
        .with_max_message_bytes(40)
        .with_max_header_bytes(48)
        .with_max_headers(3);
    let expected = vec![Err(FrameError::TooLarge {
        length: None,
        limit: 40,
    })];
    for chunk in [1, 15, 26, 41, 76, 1000] {
        assert_eq!(
            newline_frames(&limits, &input, chunk),
            expected,
            "chunk {chunk}"
        );
    }
}

#[test]
fn test_oversized_line_reports_the_same_error_however_the_input_is_split() {
    let limits = Limits::new().with_max_message_bytes(4);
    let expected = vec![
        Err(FrameError::TooLarge {
            length: None,
            limit: 4,
        }),
        Ok(b"ok".to_vec()),
    ];
    for chunk in 1..12 {
        assert_eq!(
            newline_frames(&limits, b"123456\r\nok\n", chunk),
            expected,
            "chunk {chunk}"
        );
    }
}

/// Every frame or error from newline-framed `input`, fed `chunk` bytes at a time.
fn newline_frames(limits: &Limits, input: &[u8], chunk: usize) -> Vec<Result<Vec<u8>, FrameError>> {
    let mut decoder = Framing::Newline.decoder(limits);
    let mut out = Vec::new();
    let mut rest = input;
    loop {
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => out.push(Ok(frame.to_vec())),
                Ok(None) => break,
                Err(error) => out.push(Err(error)),
            }
        }
        if rest.is_empty() {
            break;
        }
        let n = decoder.feed(&rest[..rest.len().min(chunk)]);
        rest = &rest[n..];
    }
    while let Some(next) = match decoder.finish() {
        Ok(Some(frame)) => Some(Ok(frame.to_vec())),
        Ok(None) => None,
        Err(error) => Some(Err(error)),
    } {
        let stop = next.is_err();
        out.push(next);
        if stop {
            break;
        }
    }
    out
}

#[test]
fn test_header_terminator_counts_against_the_budget_however_the_input_is_split() {
    // Found by the fuzz harness: the blank line that ends a header section
    // counted against `max_header_bytes` when it arrived in pieces (its `\r`
    // was pending) but not when it arrived whole, so the same stream was
    // refused or accepted depending on the read sizes. Terminators count.
    let limits = Limits::new().with_max_header_bytes(16);
    let input = b"ontent-type: a\r\n\r\n";
    for chunk in [1, 2, 3, 17, 100] {
        let mut decoder = Framing::ContentLength.decoder(&limits);
        let mut result = Ok(None);
        for piece in input.chunks(chunk) {
            let _taken = decoder.feed(piece);
            result = decoder.next_frame().map(|frame| frame.map(<[u8]>::to_vec));
            if result.is_err() {
                break;
            }
        }
        assert_eq!(
            result,
            Err(FrameError::HeaderTooLarge { limit: 16 }),
            "chunk {chunk}"
        );
    }
    // Within the budget, terminator included, it is read either way.
    let limits = Limits::new().with_max_header_bytes(22);
    for chunk in [1, 5, 100] {
        let mut decoder = Framing::ContentLength.decoder(&limits);
        let mut frames = Vec::new();
        for piece in b"Content-Length: 1\r\n\r\nx".chunks(chunk) {
            let _taken = decoder.feed(piece);
            while let Ok(Some(frame)) = decoder.next_frame() {
                frames.push(frame.to_vec());
            }
        }
        assert_eq!(frames, [b"x".to_vec()], "chunk {chunk}");
    }
}

/// Review 0.4 F-1: a string id had no budget of its own, and each deferred
/// request in flight kept two copies of it. A long id is now refused under
/// `null` before any copy, and the handler never runs.
#[test]
fn test_overlong_request_id_is_refused_before_it_is_kept() {
    let router = Router::new().deferred("work", |held: &mut Vec<Responder>, _, _, r| {
        held.push(r);
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, Vec::new());
    let long = "x".repeat(Limits::new().max_id_bytes);
    let body = format!(r#"{{"jsonrpc":"2.0","id":"{long}","method":"work"}}"#);
    assert!(e.handle(body.as_bytes()).is_ok());
    let out: Vec<String> = rx.try_iter().collect();
    assert_eq!(out.len(), 1);
    assert!(
        out[0].starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600"#),
        "{}",
        out[0]
    );
    assert!(e.state_mut().is_empty(), "the handler ran");
    assert_eq!(e.stats().in_flight, 0);
    // An id at the limit (as JSON text, quotes included) is accepted.
    let fits = "y".repeat(Limits::new().max_id_bytes - 2);
    let body = format!(r#"{{"jsonrpc":"2.0","id":"{fits}","method":"work"}}"#);
    assert!(e.handle(body.as_bytes()).is_ok());
    assert_eq!(e.stats().in_flight, 1);
    // The network preset is tighter.
    assert!(Limits::network().max_id_bytes < Limits::new().max_id_bytes);
}

/// Review 0.4 F-2: a batch's answers waited for its slowest deferred member
/// with no byte budget, so 164 KiB of input held 128 MiB. The answers held
/// for open batches are now bounded by `Config::max_batch_bytes`: past it a
/// member is answered with a short error, and a new batch is refused whole.
#[test]
fn test_open_batches_hold_at_most_max_batch_bytes() {
    const BUDGET: usize = 256 * 1024;
    let big = "y".repeat(32 * 1024);
    let router = Router::new()
        .deferred("slow", |held: &mut Vec<Responder>, _, _, r| held.push(r))
        .request("get", move |_, _, _| Ok(big.clone()));
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, Vec::new()).with_config(
        Config::new()
            .with_limits(Limits::network())
            .with_max_batch_bytes(BUDGET),
    );
    let batch = |b: usize| {
        let mut body = format!(r#"[{{"jsonrpc":"2.0","id":"s{b}","method":"slow"}}"#);
        for i in 0..127 {
            body.push_str(&format!(r#",{{"jsonrpc":"2.0","id":{i},"method":"get"}}"#));
        }
        body.push(']');
        body
    };
    // The first batch fills the budget and stays open behind `slow`.
    assert!(e.handle(batch(0).as_bytes()).is_ok());
    assert_eq!(rx.try_iter().count(), 0);
    // Every later batch is refused whole, at once, with one error.
    for b in 1..8 {
        assert!(e.handle(batch(b).as_bytes()).is_ok());
        let out: Vec<String> = rx.try_iter().collect();
        assert_eq!(out.len(), 1, "batch {b}");
        assert!(
            out[0].contains(r#""id":null,"error":{"code":-32000"#),
            "{}",
            out[0]
        );
    }
    assert_eq!(e.state_mut().len(), 1, "refused batches were handled");
    // Answering `slow` sends the first batch: within the budget plus one
    // short error per member past it.
    let slow = e.state_mut().pop();
    assert!(slow.is_some_and(|r| r.ok("done").is_ok()));
    let out: Vec<String> = rx.try_iter().collect();
    assert_eq!(out.len(), 1);
    assert!(out[0].len() < BUDGET + 128 * 160, "{} bytes", out[0].len());
    assert!(out[0].contains("too large to hold"));
    // With the batch sent, the budget is free again.
    assert!(e.handle(batch(9).as_bytes()).is_ok());
    assert_eq!(rx.try_iter().count(), 0);
    assert_eq!(e.state_mut().len(), 1);
}

/// Review 0.4 F-4: a malformed message that reused the id of a request still
/// being answered was answered under that id, and with a gate that error was
/// taken as the opening request's failure. It is now refused under `null`.
#[test]
fn test_invalid_message_reusing_an_in_flight_id_is_answered_under_null() {
    let router = Router::new()
        .deferred("initialize", |held: &mut Vec<Responder>, _, _, r| {
            held.push(r)
        })
        .request("hover", |_, _, _| Ok("ok"));
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router, Vec::new())
        .with_config(Config::new().with_gate(Some(Gate::new("initialize"))));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"1.0","id":1,"method":"anything"}"#)
            .is_ok()
    );
    let responder = e.state_mut().pop();
    assert!(responder.is_some_and(|r| r.ok("initialized").is_ok()));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"hover"}"#)
            .is_ok()
    );
    let out: Vec<String> = rx.try_iter().collect();
    assert_eq!(out.len(), 3, "{out:?}");
    assert!(
        out[0].contains(r#""id":null,"error":{"code":-32600"#),
        "{}",
        out[0]
    );
    assert_eq!(out[1], r#"{"jsonrpc":"2.0","id":1,"result":"initialized"}"#);
    assert_eq!(out[2], r#"{"jsonrpc":"2.0","id":2,"result":"ok"}"#);
}
