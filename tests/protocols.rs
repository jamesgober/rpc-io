//! Protocol helpers: LSP's error codes.
#![cfg(feature = "std")]

use rpc_io::ErrorCode;

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
