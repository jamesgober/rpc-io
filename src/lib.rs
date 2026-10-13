//! # rpc-io
//!
//! A JSON-RPC 2.0 engine for the protocols editors and tools speak: LSP and
//! DAP (Content-Length framing), MCP over stdio (newline framing), and service
//! APIs. Build a [`Router`] of handlers, hand it a byte stream, and the crate
//! does the rest: framing, the envelope, batches, id correlation, cancellation,
//! and the error codes the specification prescribes, with bounded buffers and
//! no panics on any input. JSON is [`jsonx_lib`], the family's own; there are
//! no third-party runtime dependencies.
//!
//! ## Quick start
//!
//! ```
//! # #[cfg(not(feature = "std"))] fn main() {}
//! # #[cfg(feature = "std")] fn main() -> Result<(), rpc_io::Error> {
//! use jsonx_lib::json;
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
//!   byte stream and frames them back. The decoder is sans-IO and holds at
//!   most one message plus a read chunk, whatever the peer sends.
//! - **Messages** ([`decode`], [`Message`], [`Incoming`], [`Invalid`]):
//!   one pass of `jsonx_lib`'s pull reader per body; `params` and `result`
//!   stay borrowed slices ([`Raw`]) until a handler parses them.
//! - **Correlation** ([`Pending`]): our outstanding requests by id, with
//!   deadlines and a capacity, every operation `O(log n)`.
//! - **Endpoint** (`Endpoint`, `Router`, `Peer`, `Call`, `Responder`): the
//!   dispatcher, both directions, with cancellation (`Cancellation`,
//!   `CancelToken`) and an opening-request `Gate`. Needs `std`.
//!
//! ## Policies
//!
//! - **Every request gets exactly one response; no notification gets one.**
//!   A malformed message is answered under its own id when that id could be
//!   read, and under `null` otherwise, as JSON-RPC 2.0 requires.
//! - **Handler panics are contained.** A request whose handler panics is
//!   answered with `INTERNAL_ERROR` (the panic message goes to the event
//!   hook, never to the peer), and the endpoint keeps serving. This needs
//!   `panic = "unwind"`, Rust's default.
//! - **Ids are strings or integers.** `null`, fractional, and out-of-range
//!   request ids are refused as invalid requests (LSP and MCP forbid them).
//!
//! ## Features
//!
//! - `std` (default): the endpoint, its handles, and the blocking adapter.
//!   Without it the crate is `no_std` (needs `alloc`): framing, the message
//!   model, and the correlation map.

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
mod queue;
#[cfg(feature = "std")]
mod router;
#[cfg(feature = "std")]
mod sink;
#[cfg(feature = "std")]
mod slot;
#[cfg(feature = "std")]
mod stats;
#[cfg(feature = "std")]
mod trace;

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
pub use handles::{Call, CancelToken, Canceller, Peer, Responder};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use queue::{Outbox, OutboxSink, QueueLimits, Queued, outbox};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use router::{Context, Event, Params, Router};
#[cfg(feature = "std")]
#[cfg_attr(docsrs, doc(cfg(feature = "std")))]
pub use sink::{Framed, Sink};
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
