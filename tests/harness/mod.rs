//! The checks behind the fuzz targets (`fuzz/fuzz_targets/*.rs`), shared with
//! the arbitrary-bytes tests in `tests/fuzz_harness.rs` so `cargo test` runs
//! them too. Each takes any bytes, drives one parser of untrusted input, and
//! panics only when an invariant breaks.
//!
//! The leading bytes of the input pick the parameters (limits, chunk sizes,
//! framing), so a fuzzer explores those along with the payload.
#![allow(dead_code)]

use std::sync::mpsc;

use rpc_io::{
    Config, Decoder, Endpoint, ErrorCode, FrameError, Framing, Incoming, Limits, Message,
    Responder, Router, decode,
};

/// Splits off the first byte (0 when there is none).
fn take(data: &[u8]) -> (u8, &[u8]) {
    match data.split_first() {
        Some((&first, rest)) => (first, rest),
        None => (0, data),
    }
}

/// Small limits, chosen by `knob`, so the fuzzer reaches every budget.
fn limits(knob: u8) -> Limits {
    let message = 1 + usize::from(knob & 0x3f) * 4; // 1..=253
    Limits::new()
        .with_max_message_bytes(message)
        .with_max_header_bytes(16 + usize::from(knob >> 6) * 24)
        .with_max_headers(1 + usize::from(knob % 5))
        .with_max_batch(usize::from(knob % 7))
        .with_max_skip_bytes(if knob & 0x80 == 0 { u64::MAX } else { 512 })
        .with_json(jsonx_lib::Limits::new().with_max_depth(1 + usize::from(knob % 9)))
}

/// One frame, or the error in its place.
type Frame = Result<Vec<u8>, FrameError>;

/// Every frame and error `decoder` cuts out of `input`, fed `chunk` bytes at
/// a time, checking the memory bound after every step.
fn frames(mut decoder: Decoder, input: &[u8], chunk: usize) -> Vec<Frame> {
    let bound = decoder.memory_bound();
    let mut out = Vec::new();
    let mut rest = input;
    loop {
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => out.push(Ok(frame.to_vec())),
                Ok(None) => break,
                Err(error) => {
                    out.push(Err(error));
                    if !error.is_recoverable() {
                        // Fatal: it repeats, and nothing more is taken.
                        assert_eq!(decoder.next_frame(), Err(error));
                        assert!(decoder.space().is_empty());
                        return out;
                    }
                }
            }
            assert!(
                decoder.capacity() <= bound,
                "{} > {bound}",
                decoder.capacity()
            );
        }
        if rest.is_empty() {
            break;
        }
        let n = decoder.feed(&rest[..rest.len().min(chunk)]);
        assert!(n > 0, "the decoder took nothing after Ok(None)");
        rest = &rest[n..];
        assert!(
            decoder.capacity() <= bound,
            "{} > {bound}",
            decoder.capacity()
        );
    }
    loop {
        match decoder.finish() {
            Ok(Some(frame)) => out.push(Ok(frame.to_vec())),
            Ok(None) => break,
            Err(error) => {
                out.push(Err(error));
                if !error.is_recoverable() {
                    break;
                }
            }
        }
    }
    out
}

/// A framing, fed in two different chunkings: never panics, stays within
/// its memory bound, yields frames within the message limit, and yields the
/// same frames and errors however the input is split.
pub fn framing(framing: Framing, data: &[u8]) {
    let (knob, data) = take(data);
    let (split, data) = take(data);
    let limits = limits(knob);
    let a = frames(framing.decoder(&limits), data, 1 + usize::from(split % 17));
    let b = frames(
        framing
            .decoder(&limits)
            .with_read_chunk(1 + usize::from(split >> 4)),
        data,
        usize::MAX,
    );
    assert_eq!(
        a,
        b,
        "the frames depend on how the input was split: knob {knob}, split {split}, {:?}",
        String::from_utf8_lossy(data)
    );
    for frame in a.iter().flatten() {
        assert!(frame.len() <= limits.max_message_bytes);
    }
}

/// `decode` on any body: never panics; batches are non-empty and within
/// `max_batch`; an invalid message is answered with `PARSE_ERROR` or
/// `INVALID_REQUEST` only; and every valid message writes back to JSON that
/// decodes to the same message.
pub fn message(data: &[u8]) {
    let (knob, body) = take(data);
    let limits = limits(knob).with_max_message_bytes(usize::MAX);
    let check = |entry: &rpc_io::Entry<'_>| match entry {
        Ok(message) => {
            let text = message.to_string();
            match decode(text.as_bytes(), &Limits::new()) {
                Incoming::Single(Ok(again)) => assert_eq!(&again, message, "{text}"),
                other => panic!("{text} decoded as {other:?}"),
            }
        }
        Err(invalid) => {
            let code = invalid.error().code();
            assert!(
                code == ErrorCode::PARSE_ERROR || code == ErrorCode::INVALID_REQUEST,
                "{code}"
            );
            assert_eq!(invalid.reply().is_none(), invalid.is_response());
        }
    };
    match decode(body, &limits) {
        Incoming::Single(entry) => check(&entry),
        Incoming::Batch(entries) => {
            assert!(!entries.is_empty());
            assert!(entries.len() <= limits.max_batch);
            entries.iter().for_each(check);
        }
    }
}

/// `decode` on `[` + any bytes + `]`: a batch, or a single refusal; never a
/// batch over `max_batch`, and none at all while batches are off.
pub fn batch(data: &[u8]) {
    let (knob, inner) = take(data);
    let limits = limits(knob).with_max_message_bytes(usize::MAX);
    let mut body = Vec::with_capacity(inner.len() + 2);
    body.push(b'[');
    body.extend_from_slice(inner);
    body.push(b']');
    match decode(&body, &limits) {
        Incoming::Batch(entries) => {
            assert!(limits.max_batch > 0, "a batch was taken with batches off");
            assert!(!entries.is_empty() && entries.len() <= limits.max_batch);
        }
        Incoming::Single(Err(invalid)) => assert!(invalid.id().is_none()),
        Incoming::Single(Ok(message)) => panic!("a batch decoded as one message: {message}"),
    }
}

/// A router of every kind of handler, for the endpoint target. With
/// `panics`, some handlers panic (libFuzzer aborts on any panic, contained or
/// not, so the fuzz target leaves them out).
fn hostile(panics: bool) -> Router<Vec<Responder>> {
    let router = Router::<Vec<Responder>>::new()
        .request("ok", |_, params, _| params.parse().map(|_| ()))
        .request("fail", |_, _, _| {
            Err::<(), _>(rpc_io::ErrorObject::new(ErrorCode(1), "no"))
        })
        .deferred("later", |held, _, _, responder| {
            if held.len() < 64 {
                held.push(responder);
            }
        })
        .deferred("now", |_, _, _, responder| {
            let _answered = responder.ok(1);
        })
        .notification("note", |_, _, cx| {
            let _sent = cx.notify("noted", ());
        })
        .notification("exit", |_, _, cx| cx.stop());
    if panics {
        router
            .request("boom", |_, _, _| -> Result<(), rpc_io::ErrorObject> {
                panic!("deliberate: fuzz")
            })
            .notification("boom", |_, _, _| panic!("deliberate: fuzz"))
    } else {
        router
    }
}

/// A live endpoint reading any bytes with either framing: never panics,
/// ends, and sends only well-formed responses and the handlers'
/// notifications.
pub fn endpoint(data: &[u8], panics: bool) {
    let (knob, data) = take(data);
    let framing = if knob & 1 == 0 {
        Framing::ContentLength
    } else {
        Framing::Newline
    };
    let mut config = Config::new()
        .with_limits(limits(knob).with_max_message_bytes(4096))
        .with_timer(false);
    if knob & 2 != 0 {
        config = config.with_gate(Some(rpc_io::Gate::new("ok").allow("note")));
    }
    if knob & 4 != 0 {
        config = config.with_cancellation(Some(rpc_io::Cancellation::mcp()));
    }
    let (tx, rx) = mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, hostile(panics), Vec::new()).with_config(config);
    let _ended = endpoint.run(data, framing);
    endpoint.state_mut().clear(); // answers every deferred request
    drop(endpoint);
    for sent in rx.try_iter() {
        match decode(sent.as_bytes(), &Limits::new()) {
            Incoming::Single(Ok(Message::Response(_))) => {}
            Incoming::Single(Ok(Message::Notification(n))) => assert_eq!(n.method, "noted"),
            Incoming::Batch(entries) => assert!(
                entries
                    .iter()
                    .all(|e| matches!(e, Ok(Message::Response(_)))),
                "{sent}"
            ),
            other => panic!("sent {other:?}: {sent}"),
        }
    }
}
