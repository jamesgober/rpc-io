//! Protocol helpers: the batches-off switch and LSP's error codes.
#![cfg(feature = "std")]

use rpc_io::{ErrorCode, Incoming, Limits, decode};

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
