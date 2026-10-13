//! Protocol presets and helpers: `Config::lsp`/`mcp`, `Gate` presets, the
//! batches-off switch, `Limits` presets, MCP progress, and LSP work-done
//! progress.
#![cfg(feature = "std")]

mod common;

use common::{endpoint, sent};
use jsonx_lib::json;
use rpc_io::{
    Config, Endpoint, ErrorCode, Gate, Incoming, Limits, Params, Progress, Raw, Router, WorkDone,
    decode,
};

fn mcp_server() -> Router<()> {
    Router::new()
        .request("initialize", |_, _, _| {
            Ok(json!({ "protocolVersion": "2025-06-18" }))
        })
        .request("ping", |_, _, _| Ok(json!({})))
        .request("tools/list", |_, _, _| Ok(json!({ "tools": [] })))
}

#[test]
fn test_mcp_config_turns_batches_off_and_serves_ping_before_initialize() {
    let (e, rx) = endpoint(mcp_server(), ());
    let mut e = e.with_config(Config::mcp());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"[{"jsonrpc":"2.0","id":3,"method":"ping"}]"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert_eq!(replies[0], r#"{"jsonrpc":"2.0","id":1,"result":{}}"#);
    assert!(replies[1].starts_with(r#"{"jsonrpc":"2.0","id":2,"error":{"code":-32002"#));
    assert_eq!(
        replies[2],
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"batches are not accepted"}}"#
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":4,"method":"initialize","params":{}}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":5,"method":"tools/list"}"#)
            .is_ok()
    );
    let replies = sent(&rx);
    assert_eq!(
        replies[1],
        r#"{"jsonrpc":"2.0","id":5,"result":{"tools":[]}}"#
    );
}

#[test]
fn test_batches_off_refuses_any_array_at_once() {
    let limits = Limits::new().without_batches();
    for body in [
        &b"[]"[..],
        b"[1,",
        b"[{\"jsonrpc\":\"2.0\",\"method\":\"x\"}]",
    ] {
        let Incoming::Single(Err(refused)) = decode(body, &limits) else {
            panic!("{body:?} was accepted");
        };
        assert_eq!(refused.error().message(), "batches are not accepted");
        assert!(refused.id().is_none());
    }
    // Single messages are unaffected.
    assert!(matches!(
        decode(br#"{"jsonrpc":"2.0","method":"x"}"#, &limits),
        Incoming::Single(Ok(_))
    ));
}

#[test]
fn test_lsp_config_gates_on_initialize_and_lets_exit_through() {
    let router = Router::new()
        .request("initialize", |_: &mut Vec<&str>, _, _| {
            Ok(json!({ "capabilities": {} }))
        })
        .request("textDocument/hover", |_, _, _| Ok(()))
        .notification("exit", |log, _, cx| {
            log.push("exit");
            cx.stop();
        });
    let (e, rx) = endpoint(router, Vec::new());
    let mut e = e.with_config(Config::lsp());
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover"}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].contains("-32002"));
    assert!(matches!(
        e.handle(br#"{"jsonrpc":"2.0","method":"exit"}"#),
        Ok(rpc_io::Flow::Stop)
    ));
    assert_eq!(e.state(), &["exit"]);
}

#[test]
fn test_gate_presets_and_allowed_requests() {
    assert_eq!(Gate::lsp().method, "initialize");
    assert!(Gate::lsp().allow_requests.is_empty());
    assert_eq!(
        Gate::mcp().allow,
        Vec::<std::borrow::Cow<'static, str>>::new()
    );
    // An allowed request is served while the opening request is answered.
    let router = Router::new()
        .deferred("auth", |held: &mut Vec<rpc_io::Responder>, _, _, r| {
            held.push(r)
        })
        .request("version", |_, _, _| Ok("1"));
    let (e, rx) = endpoint(router, Vec::new());
    let mut e =
        e.with_config(Config::new().with_gate(Some(Gate::new("auth").allow_request("version"))));
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"auth"}"#)
            .is_ok()
    );
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":2,"method":"version"}"#)
            .is_ok()
    );
    assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","id":2,"result":"1"}"#]);
}

#[test]
fn test_limits_presets_are_ordered() {
    let (default, network, strict) = (Limits::new(), Limits::network(), Limits::strict());
    assert_eq!(Limits::lsp(), default);
    assert_eq!(Limits::mcp().max_batch, 0);
    for (a, b) in [(default, network), (network, strict)] {
        assert!(a.max_message_bytes > b.max_message_bytes);
        assert!(a.max_header_bytes > b.max_header_bytes);
        assert!(a.max_headers > b.max_headers);
        assert!(a.max_batch > b.max_batch);
        assert!(a.max_skip_bytes > b.max_skip_bytes);
        assert!(a.json.max_depth > b.json.max_depth);
        assert!(a.json.max_elements > b.json.max_elements);
    }
    // Nesting the network preset takes, the strict one refuses.
    let deep = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"m","params":{}{}}}"#,
        "[".repeat(40),
        "]".repeat(40)
    );
    assert!(matches!(
        decode(deep.as_bytes(), &network),
        Incoming::Single(Ok(_))
    ));
    let Incoming::Single(Err(refused)) = decode(deep.as_bytes(), &strict) else {
        panic!("the strict preset took 40 levels");
    };
    assert_eq!(refused.error().code(), ErrorCode::INVALID_REQUEST);
}

#[test]
fn test_skip_limit_closes_a_connection_announcing_a_huge_body() {
    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut e = Endpoint::new(tx, Router::<()>::new(), ())
        .with_config(Config::new().with_limits(Limits::strict()));
    let input: &[u8] = b"Content-Length: 4194305\r\n\r\n";
    assert!(matches!(
        e.run(input, rpc_io::Framing::ContentLength),
        Err(rpc_io::Error::Frame(rpc_io::FrameError::TooLargeToSkip {
            length: 4_194_305,
            ..
        }))
    ));
    assert!(e.is_closed());
}

/// The progress token a request carries, through a handler.
fn token_of(params: &str) -> Option<String> {
    let router = Router::new().request("m", |_: &mut (), params: Params<'_>, _| {
        Ok(params.progress_token().map(|t| t.as_str().to_owned()))
    });
    let (mut e, rx) = endpoint(router, ());
    let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"m","params":{params}}}"#);
    assert!(e.handle(body.as_bytes()).is_ok());
    let reply = sent(&rx).pop().unwrap_or_default();
    let value = jsonx_lib::parse(&reply).ok()?;
    value["result"].as_str().map(str::to_owned)
}

#[test]
fn test_progress_token_is_read_strictly() {
    assert_eq!(
        token_of(r#"{"_meta":{"progressToken":"abc"}}"#).as_deref(),
        Some(r#""abc""#)
    );
    assert_eq!(
        token_of(r#"{"_meta":{"progressToken":-3}}"#).as_deref(),
        Some("-3")
    );
    for params in [
        r#"{}"#,
        r#"[1]"#,
        r#"{"_meta":5}"#,
        r#"{"_meta":{}}"#,
        r#"{"_meta":{"progressToken":1.5}}"#,
        r#"{"_meta":{"progressToken":{"a":1}}}"#,
        r#"{"_meta":{"progressToken":null}}"#,
        r#"{"_meta":{"progressToken":1,"progressToken":2}}"#,
        r#"{"_meta":{"progressToken":1},"_meta":{"progressToken":2}}"#,
    ] {
        assert_eq!(token_of(params), None, "{params}");
    }
}

#[test]
fn test_progress_reports_and_work_done_reports() {
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let Ok(token) = Raw::new(r#""t""#) else {
        panic!("not JSON")
    };
    let progress = Progress::new(endpoint.peer(), token.clone());
    assert!(progress.report(3.0, Some(10.0), None).is_ok());
    assert!(progress.report(1e300, None, Some("big\"quote")).is_ok());
    assert!(progress.report(1.0, Some(f64::INFINITY), None).is_err());
    let work = WorkDone::new(endpoint.peer(), token);
    assert!(work.begin("T", true, Some("m"), Some(250)).is_ok());
    assert!(work.report(None, None).is_ok());
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(
        sent[0],
        r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"t","progress":3,"total":10}}"#
    );
    assert!(
        sent[1].contains(r#""progress":1e300"#) || sent[1].contains(r#""progress":1e+300"#),
        "{}",
        sent[1]
    );
    assert!(sent[1].contains(r#""message":"big\"quote""#), "{}", sent[1]);
    assert_eq!(
        sent[2],
        r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"t","value":{"kind":"begin","title":"T","cancellable":true,"message":"m","percentage":100}}}"#
    );
    assert_eq!(
        sent[3],
        r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"t","value":{"kind":"report"}}}"#
    );
    assert_eq!(sent.len(), 4, "the refused report was sent: {sent:?}");
    assert!(format!("{progress:?} {work:?}").contains("token"));
}

#[test]
fn test_work_done_token_and_from_params() {
    let router = Router::new().request("m", |_: &mut (), params, cx| {
        let work = WorkDone::from_params(&params, cx.peer());
        let progress = Progress::from_params(&params, cx.peer());
        Ok(json!([work.is_some(), progress.is_some()]))
    });
    let (mut e, rx) = endpoint(router, ());
    for (params, expected) in [
        (r#"{"workDoneToken":"w"}"#, "[true,false]"),
        (r#"{"workDoneToken":2.5}"#, "[false,false]"),
        (r#"{"_meta":{"progressToken":1}}"#, "[false,true]"),
    ] {
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"m","params":{params}}}"#);
        assert!(e.handle(body.as_bytes()).is_ok());
        assert_eq!(
            sent(&rx),
            [format!(r#"{{"jsonrpc":"2.0","id":1,"result":{expected}}}"#)]
        );
    }
    // No params at all: no token.
    assert!(
        e.handle(br#"{"jsonrpc":"2.0","id":1,"method":"m"}"#)
            .is_ok()
    );
    assert!(sent(&rx)[0].ends_with(r#""result":[false,false]}"#));
}

#[test]
fn test_lsp_error_codes_have_names() {
    for (code, name) in [
        (ErrorCode::CONTENT_MODIFIED, "content modified"),
        (ErrorCode::SERVER_CANCELLED, "server cancelled"),
        (ErrorCode::REQUEST_FAILED, "request failed"),
    ] {
        assert_eq!(code.name(), Some(name));
        // LSP reserves -32899..=-32800 for itself, outside JSON-RPC's range.
        assert!(!code.is_reserved());
    }
    assert_eq!(ErrorCode::CONTENT_MODIFIED.0, -32801);
}
