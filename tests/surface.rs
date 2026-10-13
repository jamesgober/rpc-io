//! The public surface, item by item: accessors, conversions, `Display` and
//! `Debug`, defaults and builders, and the edges of the endpoint the other
//! suites do not reach (interrupted and failing reads, partial writes, limits
//! at their boundaries through the endpoint, cancellation corner cases).
#![cfg(feature = "std")]

mod common;

use std::{
    borrow::Cow,
    error::Error as _,
    io::{self, Read, Write},
    sync::mpsc,
    time::Duration,
};

use common::{Faulty, Shared, endpoint, quiet_deliberate_panics, sent};
use jsonx_lib::{Kind, Parser, json};
use rpc_io::{
    CallError, CancelToken, Cancellation, Config, End, Endpoint, Error, ErrorCode, ErrorObject,
    Event, FrameError, Framed, Framing, Gate, Id, Json, Limits, Pending, QueueLimits, Raw,
    Responder, Router, Sink,
};

// --- Errors ------------------------------------------------------------------

#[test]
fn test_error_display_and_source_for_every_variant() {
    let io = Error::Io(io::Error::new(io::ErrorKind::BrokenPipe, "pipe"));
    let frame = Error::Frame(FrameError::Truncated);
    let cases: [(Error, &str, bool); 8] = [
        (io, "the transport failed: pipe", true),
        (
            frame,
            "the input lost its framing: the stream ended inside a message",
            true,
        ),
        (Error::Closed, "the connection is closed", false),
        (
            Error::InvalidParams(Kind::Array),
            "params must be an array, an object, or null, not an array",
            false,
        ),
        (
            Error::InvalidParams(Kind::Number),
            "params must be an array, an object, or null, not a number",
            false,
        ),
        (
            Error::AlreadyAnswered,
            "the request was already answered",
            false,
        ),
        (
            Error::Backlog,
            "too much is already held until the opening request succeeds; nothing was sent",
            false,
        ),
        (
            Error::Serialize("x".into()),
            "the value is not JSON: x",
            false,
        ),
    ];
    for (error, text, has_source) in cases {
        assert_eq!(error.to_string(), text);
        assert_eq!(error.source().is_some(), has_source, "{error:?}");
        let object = ErrorObject::from(error);
        assert_eq!(object.code(), ErrorCode::INTERNAL_ERROR);
    }
    let from_io: Error = io::Error::other("x").into();
    assert!(matches!(from_io, Error::Io(_)));
    let from_frame: Error = FrameError::InvalidHeader.into();
    assert!(matches!(
        from_frame,
        Error::Frame(FrameError::InvalidHeader)
    ));
}

#[test]
fn test_call_error_display_for_every_variant() {
    for (error, text) in [
        (
            CallError::InvalidResponse(ErrorObject::invalid_request("r")),
            "the peer's answer was invalid: r (code -32600)",
        ),
        (CallError::Cancelled, "the request was cancelled"),
        (
            CallError::Evicted,
            "the request was dropped: too many requests were outstanding",
        ),
        (
            CallError::Closed,
            "the connection closed before an answer arrived",
        ),
        (
            CallError::WouldDeadlock,
            "waiting would block the endpoint's own thread, which reads the answer",
        ),
    ] {
        assert_eq!(error.to_string(), text);
        assert!(error.source().is_none());
    }
}

#[test]
fn test_frame_error_display_for_every_variant() {
    for (error, text) in [
        (
            FrameError::HeaderTooLarge { limit: 4 },
            "a header section is longer than 4 bytes",
        ),
        (
            FrameError::TooManyHeaders { limit: 2 },
            "a header section has more than 2 lines",
        ),
        (
            FrameError::InvalidHeader,
            "a header line is not `name: value`",
        ),
        (
            FrameError::MissingContentLength,
            "a header section has no `Content-Length`",
        ),
        (
            FrameError::DuplicateContentLength,
            "a header section has more than one `Content-Length`",
        ),
        (
            FrameError::InvalidContentLength,
            "a `Content-Length` value is not a non-negative decimal integer",
        ),
        (
            FrameError::TooLarge {
                length: Some(9),
                limit: 8,
            },
            "a message of 9 bytes exceeds the limit of 8",
        ),
        (
            FrameError::TooLarge {
                length: None,
                limit: 8,
            },
            "a message exceeds the limit of 8 bytes",
        ),
    ] {
        assert_eq!(error.to_string(), text);
    }
}

#[test]
fn test_error_codes_and_objects() {
    for (code, name) in [
        (ErrorCode::PARSE_ERROR, "parse error"),
        (ErrorCode::INVALID_REQUEST, "invalid request"),
        (ErrorCode::METHOD_NOT_FOUND, "method not found"),
        (ErrorCode::INVALID_PARAMS, "invalid params"),
        (ErrorCode::INTERNAL_ERROR, "internal error"),
        (ErrorCode::SERVER_OVERLOADED, "server overloaded"),
        (ErrorCode::SERVER_NOT_INITIALIZED, "server not initialized"),
        (ErrorCode::REQUEST_CANCELLED, "request cancelled"),
    ] {
        assert_eq!(code.name(), Some(name));
        // LSP's `RequestCancelled` sits below JSON-RPC's reserved range.
        assert_eq!(code.is_reserved(), code != ErrorCode::REQUEST_CANCELLED);
        assert_eq!(code.to_string(), format!("{} ({name})", code.0));
    }
    assert_eq!(ErrorCode(7).to_string(), "7");
    assert!(!ErrorCode(-31999).is_reserved() && !ErrorCode(-32769).is_reserved());
    let object = ErrorObject::method_not_found(&"m".repeat(500))
        .with_data(json!({ "k": 1 }))
        .with_data(Raw::NULL);
    assert!(object.message().len() < 200);
    assert_eq!(object.data().map(Raw::as_str), Some("null"));
    let error: &dyn std::error::Error = &object;
    assert!(error.to_string().ends_with("(code -32601)"));
}

// --- Values ------------------------------------------------------------------

#[test]
fn test_ids_convert_and_compare() {
    let owned = String::from("s");
    let ids = [
        Id::from(1i64),
        Id::from(2i32),
        Id::from(3u32),
        Id::from("a"),
        Id::from(owned),
        Id::from(Cow::Borrowed("c")),
    ];
    assert_eq!(ids[2].as_i64(), Some(3));
    assert_eq!(ids[1].as_str(), None);
    assert_eq!(ids[5].as_str(), Some("c"));
    assert_eq!(ids[4].clone().into_owned(), Id::from("s"));
    assert!(ids[0] < ids[3]);
}

#[test]
fn test_raw_and_json_values() {
    let shallow = Parser::new().limits(jsonx_lib::Limits::new().with_max_depth(1));
    assert!(Raw::new_with("[[1]]", &shallow).is_err());
    assert!(Raw::new_with("  ", &shallow).is_err());
    let raw = Raw::from_value(&json!({ "a": [1, "b"] }));
    assert_eq!(raw.to_string(), r#"{"a":[1,"b"]}"#);
    assert_eq!(format!("{raw:?}"), r#"{"a":[1,"b"]}"#);
    assert_eq!(raw.parse().map(|v| v["a"][1] == "b"), Ok(true));
    assert_eq!(raw.kind(), Kind::Object);
    let value = Json::from(json!([true]));
    assert_eq!(
        (value.kind(), value.to_string()),
        (Kind::Array, "[true]".into())
    );
    assert_eq!(format!("{value:?}"), "[true]");
    let text = String::from("x");
    let owned: Json<'static> = Json::from(text.as_str()).into_owned();
    drop(text);
    assert_eq!(owned.to_string(), r#""x""#);
    let raw_json: Json<'static> = Json::from(raw.clone()).into_owned();
    assert_eq!(raw_json.kind(), Kind::Object);
}

#[test]
fn test_pending_accessors() {
    let mut pending = Pending::new(0);
    assert_eq!(pending.capacity(), 1);
    let (id, _) = pending.issue('x', None);
    assert!(pending.contains(id) && !pending.contains(id + 1));
    assert_eq!(pending.get(id), Some(&'x'));
    assert_eq!(pending.len(), 1);
    assert!(format!("{pending:?}").contains("capacity"));
}

#[test]
fn test_limits_config_gate_and_cancellation_builders() {
    let limits = Limits::default()
        .with_max_message_bytes(10)
        .with_max_header_bytes(11)
        .with_max_headers(12)
        .with_max_batch(13)
        .with_max_skip_bytes(14)
        .with_json(jsonx_lib::Limits::new().with_max_depth(3));
    assert_eq!(
        (
            limits.max_message_bytes,
            limits.max_header_bytes,
            limits.max_headers,
            limits.max_batch,
            limits.max_skip_bytes,
            limits.json.max_depth
        ),
        (10, 11, 12, 13, 14, 3)
    );
    assert_eq!(limits.parser().get_limits().max_input_bytes, 10);
    let config = Config::default()
        .with_limits(limits)
        .with_max_pending(2)
        .with_max_in_flight(3)
        .with_max_held_bytes(4)
        .with_request_timeout(Some(Duration::from_secs(1)))
        .with_cancellation(Some(Cancellation::new("c", "i", None)))
        .with_gate(Some(Gate::new("g").with_code(ErrorCode(9)).allow("n")))
        .with_timer(false);
    assert_eq!(config.gate.as_ref().map(|g| g.code), Some(ErrorCode(9)));
    assert!(format!("{config:?}").contains("max_held_bytes: 4"));
    assert_eq!(QueueLimits::default(), QueueLimits::new());
}

// --- Handles -----------------------------------------------------------------

#[test]
fn test_handles_expose_ids_tokens_and_debug() {
    let router = Router::<Vec<Responder>>::new().deferred("later", |held, _, _, r| held.push(r));
    let (mut e, rx) = endpoint(router, Vec::new());
    let peer = e.peer();
    assert!(format!("{peer:?}").contains("closed: false"));
    let call = peer.request_with_timeout("x", (), Duration::from_secs(60));
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert_eq!(call.id(), 1);
    assert!(call.deadline().is_some());
    assert!(format!("{call:?}").starts_with("Call { id: 1"));
    let canceller = call.canceller();
    assert_eq!(canceller.id(), 1);
    assert!(format!("{canceller:?}").contains("id: 1"));

    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":"r","method":"later"}"#)
            .is_ok()
    );
    let Some(responder) = e.state_mut().pop() else {
        panic!("no responder")
    };
    assert_eq!(responder.id(), &Id::from("r"));
    assert!(!responder.token().is_cancelled() && !responder.is_cancelled());
    assert!(format!("{responder:?}").contains("cancelled: false"));
    assert!(responder.peer().notify("progress", ()).is_ok());
    assert!(
        responder
            .respond(Err::<(), _>(ErrorObject::internal_error("no")))
            .is_ok()
    );
    let replies = sent(&rx);
    assert!(
        replies
            .last()
            .is_some_and(|r| r.contains(r#""id":"r","error""#))
    );
    let token = CancelToken::default();
    token.cancel();
    assert!(token.is_cancelled());
}

#[test]
fn test_router_and_context_surface() {
    let router = Router::<Vec<String>>::default()
        .request("ctx", |log, params, cx| {
            log.push(format!(
                "{} {:?} {} {:?}",
                cx.method(),
                cx.id().and_then(|id| id.as_i64()),
                params.is_present(),
                cx
            ));
            cx.request("ask", ())?.detach();
            cx.request_with_timeout("ask", (), Duration::from_secs(60))?
                .detach();
            cx.peer().notify("via-peer", ())?;
            let kept = params.clone().into_owned();
            Ok(kept.raw().map(|r| r.as_str().to_owned()))
        })
        .notification("n", |log, params, _| {
            log.push(format!("{:?}", params.parse().map(|v| v.is_null())));
        });
    assert!(router.has_request("ctx") && router.has_notification("n"));
    assert!(!router.has_request("n"));
    assert!(format!("{router:?}").contains("ctx"));
    let (mut e, rx) = endpoint(router, Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":5,"method":"ctx","params":[1]}"#)
            .is_ok()
    );
    assert!(e.handle(br#"{"jsonrpc":"2.0","method":"n"}"#).is_ok());
    assert!(e.state()[0].starts_with("ctx Some(5) true Context"));
    assert_eq!(e.state()[1], "Ok(true)");
    let replies = sent(&rx);
    assert_eq!(replies.len(), 4);
    assert_eq!(replies[3], r#"{"jsonrpc":"2.0","id":5,"result":"[1]"}"#);
    let event = Event::Dropped { method: "m" };
    assert!(format!("{event:?}").contains("Dropped"));
}

#[test]
fn test_endpoint_accessors_and_debug() {
    let mut e = Endpoint::new(
        Framed::new(Vec::new(), Framing::Newline).with_flush_threshold(0),
        Router::<u8>::new(),
        1,
    );
    *e.state_mut() += 1;
    assert_eq!(*e.state(), 2);
    assert!(format!("{e:?}").starts_with("Endpoint"));
    assert!(e.next_deadline().is_none());
    assert!(e.peer().notify("x", ()).is_ok());
    let (threshold, framing, written) = e.with_sink(|framed| {
        let written = framed.get_mut().len();
        (framed.flush_threshold(), framed.framing(), written)
    });
    assert_eq!((threshold, framing, written), (1, Framing::Newline, 31));
    e.close();
    e.close();
    let framed = Framed::new(Vec::<u8>::new(), Framing::ContentLength);
    assert!(framed.into_inner().is_empty());
}

#[test]
fn test_string_panic_payload_reaches_the_event_hook() {
    quiet_deliberate_panics();
    let router = Router::new()
        .request(
            "boom",
            |_: &mut Vec<String>, _, _| -> Result<(), ErrorObject> {
                panic!("{}", String::from("deliberate: owned payload"))
            },
        )
        .notification("boom", |_, _, _| std::panic::panic_any(7u8))
        .on_event(|log, event| {
            if let Event::Panicked { message, .. } = event {
                log.push((*message).to_owned());
            }
        });
    let (mut e, _rx) = endpoint(router, Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"boom"}"#)
            .is_ok()
    );
    assert!(e.handle(br#"{"jsonrpc":"2.0","method":"boom"}"#).is_ok());
    assert_eq!(
        e.state(),
        &[
            "deliberate: owned payload",
            "(a panic payload that is not text)"
        ]
    );
}

// --- Reads and writes --------------------------------------------------------

/// A reader that is interrupted once, then reads `data`, then fails.
struct Flaky {
    interrupted: bool,
    data: &'static [u8],
    fail: bool,
}

impl Read for Flaky {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::Error::new(io::ErrorKind::Interrupted, "again"));
        }
        if self.data.is_empty() {
            return if self.fail {
                Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset"))
            } else {
                Ok(0)
            };
        }
        let n = buf.len().min(self.data.len());
        buf[..n].copy_from_slice(&self.data[..n]);
        self.data = &self.data[n..];
        Ok(n)
    }
}

#[test]
fn test_run_retries_interrupted_reads_and_reports_failed_ones() {
    let router = || Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    let out = Shared::default();
    let mut e = Endpoint::new(Framed::new(out.clone(), Framing::Newline), router(), ());
    let input = Flaky {
        interrupted: false,
        data: b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n",
        fail: false,
    };
    assert_eq!(e.run(input, Framing::Newline).ok(), Some(End::Eof));
    assert_eq!(
        out.text(),
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n"
    );

    let (tx, _rx) = mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, router(), ());
    let call = e.peer().request("x", ());
    let input = Flaky {
        interrupted: false,
        data: b"",
        fail: true,
    };
    assert!(matches!(
        e.run(input, Framing::Newline),
        Err(Error::Io(error)) if error.kind() == io::ErrorKind::ConnectionReset
    ));
    assert!(e.is_closed());
    assert!(call.is_ok_and(|c| c.wait() == Err(CallError::Closed)));
}

/// A writer that takes `room` bytes, then fails every write.
struct Short {
    room: usize,
    taken: Vec<u8>,
}

impl Write for Short {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.room == 0 {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "full"));
        }
        let n = buf.len().min(self.room);
        self.room -= n;
        self.taken.extend_from_slice(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn test_partial_write_closes_the_connection() {
    // `write_all` gets part of a turn's output out, then fails: the endpoint
    // reports the failure from the turn and closes.
    let router = Router::new().request("big", |_: &mut (), _, cx| {
        for _ in 0..4 {
            cx.notify("chunk", json!(["x"]))?;
        }
        Ok(())
    });
    let mut e = Endpoint::new(
        Framed::new(
            Short {
                room: 50,
                taken: Vec::new(),
            },
            Framing::Newline,
        ),
        router,
        (),
    );
    let result = e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"big"}"#);
    assert!(
        matches!(&result, Err(Error::Io(error)) if error.kind() == io::ErrorKind::BrokenPipe),
        "{result:?}"
    );
    assert!(e.is_closed());
    assert_eq!(e.with_sink(|framed| framed.get_ref().taken.len()), 50);
    assert!(matches!(e.handle(b"{}"), Err(Error::Closed)));
    // A sink that refuses at once closes it the same way, from a handle.
    let e = Endpoint::new(Faulty { panic: false }, Router::<()>::new(), ());
    assert!(matches!(e.peer().notify("x", ()), Err(Error::Io(_))));
    assert!(matches!(e.peer().request("x", ()), Err(Error::Closed)));
    // The channel sink buffers nothing, so its flush and close do nothing.
    let (mut tx, _rx) = mpsc::channel::<String>();
    assert!(tx.flush().is_ok() && Sink::close(&mut tx).is_ok());
}

// --- Limits at their boundaries, through the endpoint --------------------------

fn run_content_length(limits: Limits, wire: &[u8]) -> (Result<End, String>, Vec<String>) {
    let (tx, rx) = mpsc::channel::<String>();
    let router = Router::new()
        .request("m", |_: &mut (), _, _| Ok(1))
        .notification("n", |_, _, _| {});
    let mut e = Endpoint::new(tx, router, ()).with_config(Config::new().with_limits(limits));
    let end = e
        .run(wire, Framing::ContentLength)
        .map_err(|e| e.to_string());
    (end, rx.try_iter().collect())
}

#[test]
fn test_header_byte_budget_at_its_boundary() {
    // `Content-Length: 1\r\n\r\n` is 21 bytes, terminator included.
    let wire = b"Content-Length: 1\r\n\r\n1";
    for (limit, ok) in [(20, false), (21, true), (22, true)] {
        let (end, _) = run_content_length(Limits::new().with_max_header_bytes(limit), wire);
        assert_eq!(end.is_ok(), ok, "limit {limit}: {end:?}");
    }
}

#[test]
fn test_header_line_budget_at_its_boundary() {
    let wire = b"A: 1\r\nB: 2\r\nContent-Length: 1\r\n\r\n1";
    for (limit, ok) in [(2, false), (3, true), (4, true)] {
        let (end, _) = run_content_length(Limits::new().with_max_headers(limit), wire);
        assert_eq!(end.is_ok(), ok, "limit {limit}: {end:?}");
    }
}

#[test]
fn test_batch_budget_at_its_boundary() {
    let request = r#"{"jsonrpc":"2.0","id":1,"method":"m"}"#;
    for (count, limit) in [(2, 3), (3, 3), (4, 3)] {
        let body = format!("[{}]", vec![request; count].join(","));
        let mut wire = Vec::new();
        Framing::ContentLength.encode(body.as_bytes(), &mut wire);
        let (end, replies) = run_content_length(Limits::new().with_max_batch(limit), &wire);
        assert!(end.is_ok());
        let batched = replies[0].starts_with('[');
        assert_eq!(batched, count <= limit, "{count}: {replies:?}");
    }
}

#[test]
fn test_message_budget_at_its_boundary() {
    let body = r#"{"jsonrpc":"2.0","method":"n"}"#; // 30 bytes
    let mut wire = Vec::new();
    Framing::ContentLength.encode(body.as_bytes(), &mut wire);
    for (limit, answered) in [(29, true), (30, false), (31, false)] {
        let (end, replies) = run_content_length(Limits::new().with_max_message_bytes(limit), &wire);
        assert!(end.is_ok());
        // Over the limit, the notification is answered under `null` as too large.
        assert_eq!(!replies.is_empty(), answered, "limit {limit}: {replies:?}");
    }
}

// --- Cancellation corner cases ------------------------------------------------

#[test]
fn test_cancel_of_a_request_that_is_not_deferred_does_nothing() {
    let router = Router::new().request("now", |_: &mut (), _, _| Ok(1));
    let (mut e, rx) = endpoint(router, ());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"now"}"#)
            .is_ok()
    );
    for cancel in [
        &br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#[..],
        br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1,"id":1}}"#,
        br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":[1]}"#,
        br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":null}}"#,
    ] {
        assert!(e.handle(cancel).is_ok());
    }
    assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","id":1,"result":1}"#]);
}

#[test]
fn test_lsp_cancel_of_the_opening_request_fails_it_and_shuts_the_gate() {
    let router = Router::new()
        .deferred("initialize", |held: &mut Vec<Responder>, _, _, r| {
            held.push(r)
        })
        .request("hover", |_, _, _| Ok(()));
    let (e, rx) = endpoint(router, Vec::new());
    let mut e = e.with_config(Config::lsp());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#)
            .is_ok()
    );
    // A second opening request while the first is answered is refused.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"initialize"}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].contains("is already being answered"));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].starts_with(r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32800"#));
    // The gate is shut again: other requests are refused, a new opening served.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":3,"method":"hover"}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].contains("-32002"));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":4,"method":"initialize"}"#)
            .is_ok()
    );
    assert_eq!(e.state().len(), 2);
}

#[test]
fn test_requests_after_close_fail_and_answers_after_close_are_refused() {
    let router = Router::<Vec<Responder>>::new().deferred("later", |held, _, _, r| held.push(r));
    let (mut e, _rx) = endpoint(router, Vec::new());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"later"}"#)
            .is_ok()
    );
    let Some(responder) = e.state_mut().pop() else {
        panic!("no responder")
    };
    e.close();
    assert!(responder.is_cancelled());
    assert!(matches!(responder.ok(1), Err(Error::Closed)));
    assert!(matches!(e.peer().request("x", ()), Err(Error::Closed)));
    assert!(matches!(e.peer().notify("x", ()), Err(Error::Closed)));
}
