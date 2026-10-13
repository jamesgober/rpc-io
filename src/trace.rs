//! `tracing` events and spans for the optional `tracing` feature. Without the
//! feature the macros expand to nothing: no code, no cost.
//!
//! What is recorded: methods, ids, sizes, error codes, and the crate's own
//! messages. Never `params`, `result`, or error `data`, which can carry
//! anything the application sends, secrets included.

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
            method = %$method,
            id = $id.map(::tracing::field::display),
        )
        .entered();
    };
}

pub(crate) use {call_span, event};
