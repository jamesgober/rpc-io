//! # rpc-io
//!
//! A JSON-RPC 2.0 engine for the protocols editors and tools speak: LSP
//! (Content-Length framing), MCP over stdio (newline framing), and JSON-RPC
//! service APIs. Build a [`Router`] of handlers, hand it a byte stream, and the
//! crate does the rest: framing, the envelope, batches, id correlation,
//! cancellation, deadlines, and the error codes the specification prescribes,
//! with bounded buffers and no panics on any input. JSON is jsonx-lib, the
//! family's own, re-exported as [`jsonx`]; the default build has no
//! third-party runtime dependency.
//!
//! The Content-Length framing ([`Framing::ContentLength`], [`Decoder`]) is
//! also the base protocol of the Debug Adapter Protocol and cuts DAP messages
//! as well. DAP's messages themselves (`seq`, `type`, `command`) are not
//! JSON-RPC, so the endpoint does not serve DAP: only the framing applies.
//!
//! ## Quick start
//!
//! ```
//! # #[cfg(not(feature = "std"))] fn main() {}
//! # #[cfg(feature = "std")] fn main() -> Result<(), rpc_io::Error> {
//! use rpc_io::jsonx::json;
//! use rpc_io::{End, Framing, Router};
//!
//! let router = Router::new()
//!     .request("add", |_, params, _| {
//!         let p = params.parse()?;
//!         match (p[0].as_i64(), p[1].as_i64()) {
//!             (Some(a), Some(b)) => Ok(json!(a + b)),
//!             _ => Err(rpc_io::ErrorObject::invalid_params("expected [a, b]")),
//!         }
//!     })
//!     .notification("exit", |_, _, cx| cx.stop());
//!
//! let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"add\",\"params\":[2,3]}\n\
//!                      {\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n";
//! // Real servers pass `std::io::stdin().lock()` and `std::io::stdout()`.
//! let end = rpc_io::serve(input, std::io::sink(), Framing::Newline, router)?;
//! assert_eq!(end, End::Stopped);
//! # Ok(())
//! # }
//! ```
//!
//! ## Layers
//!
//! - **Framing** ([`Framing`], [`Decoder`]): cuts message bodies out of a
//!   byte stream and frames them back. The decoder is sans-IO, holds at most
//!   one message plus a read chunk whatever the peer sends, and gives memory
//!   back after a large message.
//! - **Messages** ([`decode`], [`Message`], [`Incoming`], [`Invalid`]):
//!   one pass of `jsonx_lib`'s pull reader per body; `params` and `result`
//!   stay borrowed slices ([`Raw`]) until a handler parses them.
//! - **Correlation** ([`Pending`]): our outstanding requests by id, with
//!   deadlines and a capacity, every operation `O(log n)`.
//! - **Endpoint** (`Endpoint`, `Router`, `Peer`, `Call`, `Responder`): the
//!   dispatcher, both directions, with cancellation (`Cancellation`,
//!   `CancelToken`), an opening-request `Gate`, self-firing deadlines, an
//!   interceptor, and `Stats`. `run` reads a blocking stream; `next_read`,
//!   `commit`, and `finish` take it apart for any other kind; `run_async`
//!   reads an async one (`futures-io`). `Call` is a `Future`. Needs `std`.
//! - **Output** (`Sink`, `Framed`, `Queued`, `outbox`): where messages go.
//!   `Queued` writes from its own thread and `outbox` from an async task,
//!   behind `QueueLimits`, so a peer that stops reading blocks no sender
//!   while the connection is open; closing waits for the drain outside the
//!   endpoint's lock, for at most `QueueLimits::close_timeout`.
//!   Needs `std`.
//! - **Serving** (`serve`, and `Server` with the `net` feature): one call
//!   over a byte stream, or an accept loop over TCP and Unix sockets.
//!
//! ## Policies
//!
//! - **Every request gets exactly one response; no valid notification gets
//!   one.** A malformed message is answered under its own id when that id
//!   could be read, and under `null` otherwise, as JSON-RPC 2.0 requires.
//! - **Handler panics are contained.** A request whose handler panics is
//!   answered with `INTERNAL_ERROR` (the panic message goes to the event
//!   hook, never to the peer), and the endpoint keeps serving. This needs
//!   `panic = "unwind"`, Rust's default.
//! - **Ids are strings or integers.** `null`, fractional, and out-of-range
//!   request ids are refused as invalid requests (LSP and MCP forbid them).
//!
//! ## Features
//!
//! - `std` (default): the endpoint, its handles, `serve`, the queued and
//!   async-drained sinks, and the progress helpers. Without it the crate is
//!   `no_std` (needs `alloc`): framing, the message model, and the
//!   correlation map.
//! - `serde`: typed params and results ([`Router::request_typed`],
//!   `Params::deserialize`, `Peer::request_typed`, `Call::wait_typed`),
//!   through `serde` and `serde_json`.
//! - `futures-io`: the async adapter over `futures-io`'s traits
//!   (`Endpoint::run_async`, `Outbox::write_to`); runtime-agnostic.
//! - `tracing`: spans for every call and events for what the endpoint does
//!   on its own; without it the instrumentation compiles to nothing.
//! - `net`: `Server`, an accept loop over TCP and Unix domain sockets.
//!
//! Every optional feature is off by default, and adds only the crates it
//! names and their own dependencies.

#![cfg_attr(not(feature = "std"), no_std)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![forbid(unsafe_code)]
#![deny(
    warnings,
    missing_docs,
    unsafe_op_in_unsafe_fn,
    unused_must_use,
    unused_results,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::todo,
    clippy::unimplemented,
    clippy::panic,
    clippy::unreachable,
    clippy::dbg_macro,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

extern crate alloc;

#[cfg(feature = "futures-io")]
mod aio;
mod convert;
mod error_object;
mod framing;
mod id;
mod limits;
mod message;
mod pending;
mod raw;
mod text;

#[cfg(feature = "std")]
mod config;
#[cfg(feature = "std")]
mod endpoint;
#[cfg(feature = "std")]
mod error;
#[cfg(feature = "std")]
mod handles;
#[cfg(feature = "std")]
mod link;
#[cfg(feature = "std")]
mod progress;
#[cfg(feature = "std")]
mod queue;
#[cfg(feature = "std")]
mod router;
#[cfg(feature = "net")]
mod server;
#[cfg(feature = "std")]
mod sink;
#[cfg(feature = "std")]
mod slot;
#[cfg(feature = "std")]
mod stats;
#[cfg(feature = "std")]
mod trace;
#[cfg(feature = "serde")]
mod typed;

/// jsonx-lib, the JSON this crate is built on, re-exported: its types are
/// part of this crate's API ([`Raw::parse`] returns a [`jsonx::Value`],
/// [`Json`] converts from one, typed handlers take [`jsonx::ToJson`]), so a
/// crate on top (an LSP or MCP server) uses them through `rpc_io::jsonx` and
/// always gets the same jsonx-lib version as this crate, with no dependency
/// of its own to keep in step. `rpc_io::jsonx::json!` builds values.
///
/// # Examples
///
/// ```
/// use rpc_io::jsonx::{self, json};
///
/// let value = json!({ "line": 3 });
/// assert_eq!(jsonx::to_string(&value), r#"{"line":3}"#);
/// assert_eq!(rpc_io::Raw::from_value(&value).as_str(), r#"{"line":3}"#);
/// ```
pub use jsonx_lib as jsonx;

pub use error_object::{ErrorCode, ErrorObject};
pub use framing::{Decoder, FrameError, Framing, READ_CHUNK};
pub use id::Id;
pub use limits::Limits;
pub use message::{
    Entry, Incoming, Invalid, Message, Notification, Request, Response, decode, write_batch,
};
pub use pending::Pending;
pub use raw::{Json, Raw};

#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use config::{Cancellation, Config, Gate};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use endpoint::{End, Endpoint, Flow, Next, serve};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use error::{CallError, Error};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use handles::{Call, CancelToken, Canceller, Notifier, Peer, Responder};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use progress::{Progress, WorkDone};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use queue::{Outbox, OutboxSink, QueueLimits, Queued, outbox};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use router::{Context, Event, Params, Router};
#[cfg(feature = "net")]
#[cfg_attr(docsrs, doc(cfg(feature = "net")))]
pub use server::{Connection, Server, Stopper};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use sink::{Drain, Framed, Sink};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use stats::Stats;

/// Compiles and runs the `rust` code blocks in `README.md` and `docs/API.md` as
/// part of `cargo test`, so the published examples cannot drift from the API.
///
/// Present only while collecting doctests (`#[cfg(doctest)]`); it is not part of
/// the public surface and does not appear in the built library or its docs.
#[cfg(all(doctest, feature = "std"))]
#[doc = include_str!("../README.md")]
#[doc = include_str!("../docs/API.md")]
pub struct MarkdownDocTests;
