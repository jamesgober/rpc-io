//! `tracing` events and spans for the optional `tracing` feature. Without the
//! feature the macros expand to nothing: no code, no cost.
//!
//! What is recorded: methods, ids, sizes, error codes, error messages, and
//! panic messages. Never `params`, `result`, or error `data` as such, which
//! can carry anything the application sends, secrets included. Text the peer
//! chose (methods, string ids, and error messages that quote a value it
//! sent) is clipped to a few hundred bytes and recorded with `Debug`, which
//! escapes control characters, so it cannot flood the log or forge lines in
//! it.

/// A `tracing` event under the `rpc_io` target, at `$level`.
macro_rules! event {
    ($level:ident, $($arg:tt)+) => {
        #[cfg(feature = "tracing")]
        {
            ::tracing::$level!(target: "rpc_io", $($arg)+);
        }
    };
}

/// Enters a debug span for the call being handled, for the rest of the
/// enclosing block.
macro_rules! call_span {
    ($kind:literal, $method:expr, $id:expr) => {
        #[cfg(feature = "tracing")]
        let _span = ::tracing::debug_span!(
            target: "rpc_io",
            $kind,
            method = ?$crate::error_object::clip(&$method),
            id = $id.map(|id| ::tracing::field::debug($crate::error_object::clip(&id.to_string()).into_owned())),
        )
        .entered();
    };
}

pub(crate) use {call_span, event};
