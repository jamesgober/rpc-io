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
