<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <b>rpc-io</b>
    <br>
    <sub><sup>JSON-RPC 2.0 TRANSPORT</sup></sub>
</h1>

<div align="center">
    <a href="https://crates.io/crates/rpc-io"><img alt="Crates.io" src="https://img.shields.io/crates/v/rpc-io"></a>
    <a href="https://crates.io/crates/rpc-io"><img alt="Downloads" src="https://img.shields.io/crates/d/rpc-io?color=%230099ff"></a>
    <a href="https://docs.rs/rpc-io"><img alt="docs.rs" src="https://img.shields.io/docsrs/rpc-io"></a>
    <a href="https://github.com/jamesgober/rpc-io/actions"><img alt="CI" src="https://github.com/jamesgober/rpc-io/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
</div>

<br>

<div align="left">
    <p>
        <strong>rpc-io</strong> is a JSON-RPC 2.0 engine for the protocols editors and tools speak: LSP over Content-Length framing, MCP over newline framing (its stdio transport), and JSON-RPC service APIs over either. Register handlers on a <code>Router</code>, hand it a byte stream, and the crate does the rest: framing, the envelope, batches, id correlation, cancellation in both directions, an opening-request gate, deadlines, and the error codes the specification prescribes. Its Content-Length framing layer also cuts Debug Adapter Protocol messages, which use the same base protocol; DAP's messages are not JSON-RPC, so for DAP only the framing applies, not the engine.
    </p>
    <p>
        It is built to take hostile input. Header sections, bodies, batches, and JSON nesting all have budgets checked before memory is spent; nothing recurses on input; every request gets exactly one response and no notification gets one; and a handler that panics is answered with <code>INTERNAL_ERROR</code> while the session carries on. It is the transport of <code>lsp-lang</code> 2.0; the other LexerSketch servers are planned to move onto it.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). <code>#![forbid(unsafe_code)]</code>. One dependency, from the family: <a href="https://crates.io/crates/jsonx-lib"><code>jsonx-lib</code></a>; no third-party runtime dependencies. Without the default <code>std</code> feature the framing, message, and correlation layers are <code>no_std</code> (they need only <code>alloc</code>).
    </p>
    <blockquote>
        <strong>Status: pre-1.0.</strong> 0.2.0 is the foundation; the public API is refined across the 0.x series and frozen at <code>1.0.0</code>, after lsp-lang and mcp-io have run on it. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> and <a href="./dev/ROADMAP.md"><code>dev/ROADMAP.md</code></a>.
    </blockquote>
</div>

<hr>
<br>

## The model

Four layers, each usable on its own:

- **Framing.** [`Framing`](./docs/API.md#framing) names the wire format; a [`Decoder`](./docs/API.md#decoder) cuts message bodies out of a byte stream (sans-IO: you read, it buffers at most one message) and `Framing::encode` frames them back.
- **Messages.** [`decode`](./docs/API.md#decode) reads a body into [`Message`](./docs/API.md#message)s (`Request`, `Notification`, `Response`), a batch, or an [`Invalid`](./docs/API.md#invalid) that says how to answer it. `params` and `result` stay borrowed [`Raw`](./docs/API.md#raw) slices of the body until a handler parses them.
- **Correlation.** [`Pending`](./docs/API.md#pending) maps our outstanding request ids to waiters, with deadlines and a capacity; every operation is `O(log n)`.
- **Endpoint.** An [`Endpoint`](./docs/API.md#endpoint) routes the peer's calls to a [`Router`](./docs/API.md#router)'s handlers and settles answers to our own requests, sent through a [`Peer`](./docs/API.md#peer) from any thread. [`serve`](./docs/API.md#serve) is the one-call server.

<br>

What it guarantees, and how each guarantee is checked:

| Guarantee | How it is held |
|---|---|
| Every request gets exactly one response; no notification ever gets one. | A property test sends random batches mixing requests (answered at once, later, by a panicking handler, for a missing method, or cancelled), notifications, malformed elements, and stray responses, and checks the reply holds each request id exactly once with the right code. |
| Malformed messages get the specification's codes (`-32700`, `-32600`, `-32601`, `-32602`, `-32603`), under their own id when it can be read. | Every example in section 7 of the JSON-RPC 2.0 specification is a test, verbatim; a property test checks envelope classification against a reference built on a full parse. |
| Every message shape survives encoding and decoding, through either framing. | Property tests over random requests, notifications, results, and errors (any id, any string, any JSON), fed through the decoder in random chunk sizes. |
| No input makes it panic, and memory stays bounded. | Property tests feed arbitrary and JSON-shaped bytes to the decoder, the message decoder, and a live endpoint; the decoder's buffer is checked against its documented bound after every step. |
| A handler panic is contained. | Tests panic in request, deferred, and notification handlers, in batches, in the event hook, and in the sink, and check the answers and that the session goes on (or, for the sink, closes cleanly). |
| Cancellation races resolve to one outcome. | Tests race worker answers against peer cancellations, and our cancellations against the peer's answers, hundreds of times each. |
| No quadratic paths. | Tripwire tests: 4 MiB bodies and lines fed a byte at a time, 100,000 messages through one endpoint, a full batch of cancelled deferred requests, 50,000 forged answers while 50,000 requests are held. |

<hr>
<br>

## Installation

```toml
[dependencies]
rpc-io = "0.2"
jsonx-lib = "0.2"   # to build and read JSON values
```

The framing, message, and correlation layers without the standard library:

```toml
[dependencies]
rpc-io = { version = "0.2", default-features = false }
```

<hr>
<br>

## Quick start

A server on stdin and stdout is one call. This one takes newline-delimited messages, as MCP's stdio transport does; LSP uses `Framing::ContentLength`:

```rust
use jsonx_lib::json;
use rpc_io::{End, ErrorObject, Framing, Router};

let router = Router::new()
    .request("add", |_, params, _| {
        let p = params.parse()?; // a malformed `params` is answered with -32602
        match (p[0].as_i64(), p[1].as_i64()) {
            (Some(a), Some(b)) => Ok(json!(a + b)),
            _ => Err(ErrorObject::invalid_params("expected [a, b]")),
        }
    })
    .notification("exit", |_, _, cx| cx.stop());

// A real server passes `std::io::stdin().lock()` and `std::io::stdout()`.
let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"add\",\"params\":[2,3]}\n\
                     {\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n";
let end = rpc_io::serve(input, std::io::sink(), Framing::Newline, router)?;
assert_eq!(end, End::Stopped);
# Ok::<(), rpc_io::Error>(())
```

### State, and calls in the other direction

An [`Endpoint`](./docs/API.md#endpoint) carries state for its handlers and sends requests of its own. Answers are matched to calls by id, and a `Call` can be waited on from any thread but the endpoint's:

```rust
use std::sync::mpsc;
use jsonx_lib::json;
use rpc_io::{Endpoint, Router};

let router = Router::new().notification("textDocument/didOpen", |open: &mut Vec<String>, params, cx| {
    if let Ok(p) = params.parse() {
        open.push(p["textDocument"]["uri"].as_str().unwrap_or_default().to_owned());
    }
    // Notifications and requests go back through the context.
    cx.notify("window/logMessage", json!({ "type": 3, "message": "opened" })).ok();
});
let (tx, _rx) = mpsc::channel::<String>(); // any `Sink`: a framed stdout, a socket, a channel
let mut endpoint = Endpoint::new(tx, router, Vec::new());

endpoint.handle(br#"{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///a.rs"}}}"#)?;
assert_eq!(endpoint.state(), &["file:///a.rs"]);

let call = endpoint.peer().request("workspace/configuration", json!({ "items": [] }))?;
endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[{"tabSize":4}]}"#)?;
assert_eq!(call.wait().map(|r| r.as_str().to_owned()), Ok(r#"[{"tabSize":4}]"#.to_owned()));
# Ok::<(), rpc_io::Error>(())
```

### Long work, answered later and cancellable

A deferred handler gets a `Responder` it can keep or move to a worker. If the peer cancels (`$/cancelRequest` in LSP, `notifications/cancelled` in MCP), the responder's token is set and the request is answered as the protocol says; a late answer is refused:

```rust
use std::sync::mpsc;
use rpc_io::{Endpoint, Responder, Router};

let router = Router::new().deferred("index", |pending: &mut Vec<Responder>, _, _, responder| {
    pending.push(responder); // or: std::thread::spawn(move || { /* ... */ responder.ok(1) });
});
let (tx, rx) = mpsc::channel::<String>();
let mut endpoint = Endpoint::new(tx, router, Vec::new());
endpoint.handle(br#"{"jsonrpc":"2.0","id":7,"method":"index"}"#)?;
endpoint.handle(br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":7}}"#)?;

let reply = rx.try_recv().unwrap_or_default();
assert!(reply.starts_with(r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32800"#));
let responder = endpoint.state_mut().pop();
assert!(responder.is_some_and(|r| r.is_cancelled() && r.ok("too late").is_err()));
# Ok::<(), rpc_io::Error>(())
```

### The lower layers

```rust
use rpc_io::{decode, Framing, Incoming, Limits, Message};

let mut decoder = Framing::ContentLength.decoder(&Limits::new());
decoder.feed(b"Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}");
let body = decoder.next_frame()?.unwrap_or_default();
let Incoming::Single(Ok(Message::Request(request))) = decode(body, &Limits::new()) else {
    panic!("expected a request");
};
assert_eq!((request.method.as_ref(), request.id.as_i64()), ("ping", Some(1)));
# Ok::<(), rpc_io::FrameError>(())
```

<hr>
<br>

## Examples

Runnable programs in [`examples/`](./examples):

| Example | What it shows |
|---|---|
| [`stdio_echo`](./examples/stdio_echo.rs) | A server on stdin and stdout, newline framing (MCP) or `--content-length` (LSP): `echo '{"jsonrpc":"2.0","id":1,"method":"ping"}' \| cargo run --example stdio_echo` |
| [`pipe_pair`](./examples/pipe_pair.rs) | A client and a server over an in-memory pipe, each on its own thread: concurrent requests correlated by id, a deferred answer with progress notifications, and a cancelled request. |

<hr>
<br>

## Performance

Decoding is one pass of `jsonx-lib`'s pull reader over each body: the envelope members are read as raw slices, so `params` and `result` are validated once and never copied or decoded unless a handler asks. Method names and string ids borrow from the body when they have no escapes. Messages are written into a reused buffer and framed with one write and one flush per turn, however many messages a handler sends. The decoder reads straight into its own buffer, searches for line ends eight bytes at a time, and resumes each search where the last one stopped.

Measured with the benchmarks in [`benches/`](./benches), x86_64 Windows, Rust stable, release profile, on LSP-shaped `textDocument/hover` requests (about 160 bytes each):

| Benchmark | What it measures | Windows |
|---|---|---:|
| `framing/decode_100k_ContentLength` | Cut 100,000 Content-Length frames out of one 19 MiB stream. | ~5.1 GiB/s (3.6 ms) |
| `framing/decode_100k_Newline` | The same with newline framing. | ~7.6 GiB/s (2.2 ms) |
| `framing/encode_100k_*` | Frame 100,000 bodies (Content-Length / newline). | ~2.6 / ~2.0 GiB/s |
| `messages/decode_100k_requests` | Decode 100,000 requests: envelope, id, method, `params` validated. | ~2.3 M/s (~440 ns each) |
| `messages/decode_batch_1024` | Decode a full batch of 1,024 requests. | ~320 µs |
| `messages/decode_and_write_100k_responses` | Decode 100,000 responses and write each back. | ~2.6 M/s |
| `endpoint/handle_100k_requests` | The whole server path, 100,000 times: decode, route, parse `params`, build a result, encode, frame, write. | ~615 K/s (~1.6 µs each) |
| `endpoint/run_100k_framed_requests` | The same, read from one Content-Length stream by `run`. | ~560 K/s |
| `pending/issue_then_settle_100k_out_of_order` | Issue 100,000 requests with deadlines, settle them in scrambled order. | ~1.8 M pairs/s |

```bash
cargo bench --bench bench
```

Criterion writes per-benchmark reports to `target/criterion/`. Numbers vary by CPU; use the trend across runs, not a single absolute.

<hr>
<br>

## Design notes

- **Exactly one response.** A deferred request is tracked until answered; a `Responder` dropped without answering (a worker returned early, a panic unwound through it) answers `INTERNAL_ERROR` itself. A cancelled request is answered by the endpoint (`-32800` for LSP) or not at all (MCP), and a late answer from the worker returns `Error::AlreadyAnswered` instead of reaching the wire.
- **Ids.** Request ids are strings or integers within `i64`. `null`, fractions, and other values are refused as invalid requests, as LSP and MCP require. A request whose id is still being answered by a deferred handler is refused under a `null` id, so the only response carrying that id is the real answer. Our own ids are positive integers counted from 1; answers to ids we are not waiting on are reported and dropped.
- **Malformed messages.** A message with a `method` is answered under its own id when that id is valid and appears once; anything unreadable is answered under `null`, as the specification requires. A broken *response* is never answered (an error under its id would read as an answer to one of the peer's requests); it ends the call it names instead.
- **Budgets.** `Limits` caps message bytes (64 MiB), header bytes (8 KiB), header lines (32), batch size (1024), and every JSON budget of `jsonx-lib` (depth 128 by default). An oversized message is skipped as it arrives, never buffered, and answered; a broken frame ends the connection, since no reliable boundary is left.
- **Panics.** Handler and event-hook panics are caught; so are panics in a `Sink`, which close the connection as an I/O error. This needs `panic = "unwind"`, Rust's default.
- **Threads.** Handlers run on the endpoint's thread, one at a time, in arrival order. `Peer`, `Call`, `Responder`, and `Canceller` are `Send` and work from any thread; the endpoint's lock is held only to encode a message and hand it to the sink, never while user code runs. A message sent from a worker goes out at once, even while a handler runs.

<hr>
<br>

## Testing

```bash
cargo test                       # unit + integration + property + doctests
cargo test --no-default-features # the no_std + alloc layers
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench bench
```

[`tests/properties.rs`](./tests/properties.rs) holds the crate to independent references: a classifier built on a full `jsonx-lib` parse decides what every random envelope should decode to, and random batches check one response per request id. [`tests/endpoint.rs`](./tests/endpoint.rs) runs section 7 of the JSON-RPC 2.0 specification verbatim, [`tests/framing.rs`](./tests/framing.rs) runs the framings against malformed and hostile streams, [`tests/session.rs`](./tests/session.rs) covers cancellation, timeouts, the gate, a client and server over a pipe, and races, and [`tests/adversarial.rs`](./tests/adversarial.rs) holds one regression test per finding of the adversarial review. Every `rust` example in this README and in [`docs/API.md`](./docs/API.md) is compiled and run as a doctest.

<hr>
<br>

## Cross-platform support

- Linux (x86_64, aarch64)
- macOS (x86_64, Apple Silicon)
- Windows (x86_64)

The crate uses only `std`'s threads, channels, and clocks, and no platform-specific code.

<hr>
<br>

## Contributing

See [`REPS.md`](./REPS.md) for the engineering standards every change is held to, [`dev/DIRECTIVES.md`](./dev/DIRECTIVES.md) for the definition of done, and [`dev/ROADMAP.md`](./dev/ROADMAP.md) for what comes next. Before a PR: `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean.

<br>

<div id="license">
    <h2>License</h2>
    <p>Licensed under either of</p>
    <ul>
        <li><b>Apache License, Version 2.0</b> &mdash; <a href="./LICENSE-APACHE">LICENSE-APACHE</a></li>
        <li><b>MIT License</b> &mdash; <a href="./LICENSE-MIT">LICENSE-MIT</a></li>
    </ul>
    <p>at your option.</p>
</div>

<div align="center">
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>James Gober.</strong></sup>
</div>
