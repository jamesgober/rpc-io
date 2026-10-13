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
        It is built to take hostile input. Header sections, bodies, batches, and JSON nesting all have budgets checked before memory is spent; nothing recurses on input; every request gets exactly one response; a handler that panics is answered with <code>INTERNAL_ERROR</code> while the session carries on; and a peer that stops reading costs no more memory than its outgoing queue's limits, and its connection is closed at the next send once the writer has made no progress for the queue's write timeout, instead of freezing the server. Every parser of untrusted input has a fuzz target. <code>lsp-lang</code> 2.0 runs on its 0.2 release; the other LexerSketch servers are planned to move onto it.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). <code>#![forbid(unsafe_code)]</code>. One required dependency, from the family: <a href="https://crates.io/crates/jsonx-lib"><code>jsonx-lib</code></a>. The integrations (<code>serde</code>, <code>futures-io</code>, <code>tracing</code>) are optional features, off by default. Without the default <code>std</code> feature the framing, message, and correlation layers are <code>no_std</code> (they need only <code>alloc</code>).
    </p>
    <blockquote>
        <strong>Status: pre-1.0.</strong> 0.3.0 hardens the foundation and adds the async adapter, typed params, and serving helpers; the public API is refined across the 0.x series and frozen at <code>1.0.0</code>, after lsp-lang and mcp-io have run on it. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> and <a href="./dev/ROADMAP.md"><code>dev/ROADMAP.md</code></a>.
    </blockquote>
</div>

<hr>
<br>

## The model

Five layers, each usable on its own:

- **Framing.** [`Framing`](./docs/API.md#framing) names the wire format; a [`Decoder`](./docs/API.md#decoder) cuts message bodies out of a byte stream (sans-IO: you read, it buffers at most one message, and gives memory back after a large one) and `Framing::encode` frames them back.
- **Messages.** [`decode`](./docs/API.md#decode) reads a body into [`Message`](./docs/API.md#message)s (`Request`, `Notification`, `Response`), a batch, or an [`Invalid`](./docs/API.md#invalid) that says how to answer it. `params` and `result` stay borrowed [`Raw`](./docs/API.md#raw) slices of the body until a handler parses them.
- **Correlation.** [`Pending`](./docs/API.md#pending) maps our outstanding request ids to waiters, with deadlines and a capacity; every operation is `O(log n)`.
- **Endpoint.** An [`Endpoint`](./docs/API.md#endpoint) routes the peer's calls to a [`Router`](./docs/API.md#router)'s handlers and settles answers to our own requests, sent through a [`Peer`](./docs/API.md#peer) from any thread. [`serve`](./docs/API.md#serve) is the one-call server; [`Server`](./docs/API.md#server) serves a listener.
- **Output.** A [`Sink`](./docs/API.md#sink-and-framed) takes each outgoing message. [`Queued`](./docs/API.md#queued-and-outbox) writes from its own thread and an [`outbox`](./docs/API.md#queued-and-outbox) from an async task, both behind bounded limits, so a slow peer blocks no sender while the connection is open. Closing waits for the queue to drain only after releasing the endpoint's lock, and for at most `QueueLimits::close_timeout`.

<br>

What it guarantees, and how each guarantee is checked:

| Guarantee | How it is held |
|---|---|
| Every request gets exactly one response; no valid notification gets one (a malformed or oversized message is answered under `null`, as the specification requires). | A property test sends random batches mixing requests (answered at once, later, by a panicking handler, for a missing method, or cancelled), notifications, malformed elements, and stray responses, and checks the reply holds each request id exactly once with the right code. |
| Malformed messages get the specification's codes (`-32700`, `-32600`, `-32601`, `-32602`, `-32603`), under their own id when it can be read. | Every example in section 7 of the JSON-RPC 2.0 specification is a test, verbatim; a property test checks envelope classification against a reference built on a full parse. |
| Every message shape survives encoding and decoding, through either framing. | Property tests over random requests, notifications, results, and errors (any id, any string, any JSON), fed through the decoder in random chunk sizes. |
| No input makes it panic, memory stays bounded, and the framing does not depend on how the input is split. | Fuzz targets for both framings, message and batch decoding, and a live endpoint (`fuzz/`), whose checks also run in `cargo test` on random, JSON-shaped, and mutated inputs; the decoder's buffer is checked against its documented bound after every step. |
| A peer that stops reading stalls no sender, and closing over it holds up no other thread. | Tests block the writer and check that every handle still sends, that the connection closes once the queue's byte, message, or no-progress limit is reached, that a slow but steady reader is not cut off, and that while a close waits on a stalled writer a `Responder` on another thread returns at once. |
| Deadlines fire on their own. | Tests let a request time out with no traffic and no waiter and check the cancel notification goes out. |
| A handler panic is contained. | Tests panic in request, deferred, and notification handlers, the interceptor, batches, the event hook, the sink, and the writer, and check the answers and that the session goes on (or, for the output, closes cleanly). |
| Cancellation races resolve to one outcome. | Tests race worker answers against peer cancellations, and our cancellations against the peer's answers, hundreds of times each. |
| No quadratic paths. | Tripwire tests: 4 MiB bodies and lines fed a byte at a time, 100,000 messages through one endpoint, a full batch of cancelled deferred requests, 50,000 forged answers while 50,000 requests are held. |

<hr>
<br>

## Installation

```toml
[dependencies]
rpc-io = "0.3"
jsonx-lib = "0.2"   # to build and read JSON values
```

The framing, message, and correlation layers without the standard library:

```toml
[dependencies]
rpc-io = { version = "0.3", default-features = false }
```

| Feature | Default | Adds |
|---|---|---|
| `std` | yes | The endpoint, its handles, `serve`, `Queued`, `outbox`, progress helpers. |
| `serde` | no | Typed params and results: `Router::request_typed`, `Params::deserialize`, `Peer::request_typed`, `Call::wait_typed` (with `serde` and `serde_json`). |
| `futures-io` | no | The async adapter: `Endpoint::run_async`, and `Outbox` as `AsyncRead`/`AsyncBufRead` with `write_to`. Runtime-agnostic. |
| `tracing` | no | Spans for every call and events for what the endpoint does on its own. Off, the instrumentation compiles to nothing. |
| `net` | no | `Server`: an accept loop over TCP and, on Unix, Unix domain sockets. |

<hr>
<br>

## Quick start

A server on stdin and stdout is one call. This one takes newline-delimited messages, as MCP's stdio transport does; LSP uses `Framing::ContentLength`:

```rust
use jsonx_lib::json;
use rpc_io::{End, ErrorObject, Framing, Router};

fn main() -> Result<(), rpc_io::Error> {
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
    Ok(())
}
```

`serve` writes through a [`Queued`](./docs/API.md#queued-and-outbox) sink: a writer thread does the writes, so a client that stops reading does not block a handler's sends. The connection closes once the queue is full (64 MiB by default), or at the first send after the writer has written nothing for 60 seconds. When the input ends, `serve` waits for the queue to drain, for at most 10 seconds (`QueueLimits::close_timeout`).

### State, and calls in the other direction

An [`Endpoint`](./docs/API.md#endpoint) carries state for its handlers and sends requests of its own. Answers are matched to calls by id; a `Call` can be waited on from any thread but the endpoint's, polled, or `.await`ed:

```rust
use std::sync::mpsc;
use jsonx_lib::json;
use rpc_io::{Endpoint, Router};

fn main() -> Result<(), rpc_io::Error> {
    let router = Router::new().notification("textDocument/didOpen", |open: &mut Vec<String>, params, cx| {
        if let Ok(p) = params.parse() {
            open.push(p["textDocument"]["uri"].as_str().unwrap_or_default().to_owned());
        }
        // Notifications and requests go back through the context.
        cx.notify("window/logMessage", json!({ "type": 3, "message": "opened" })).ok();
    });
    let (tx, _rx) = mpsc::channel::<String>(); // any `Sink`: a queued stdout, a socket, a channel
    let mut endpoint = Endpoint::new(tx, router, Vec::new());

    endpoint.handle(br#"{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///a.rs"}}}"#)?;
    assert_eq!(endpoint.state(), &["file:///a.rs"]);

    let call = endpoint.peer().request("workspace/configuration", json!({ "items": [] }))?;
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[{"tabSize":4}]}"#)?;
    assert_eq!(call.wait().map(|r| r.as_str().to_owned()), Ok(r#"[{"tabSize":4}]"#.to_owned()));
    Ok(())
}
```

### Long work, answered later and cancellable

A deferred handler gets a `Responder` it can keep or move to a worker. If the peer cancels (`$/cancelRequest` in LSP, `notifications/cancelled` in MCP), the responder's token is set and the request is answered as the protocol says; a late answer is refused:

```rust
use std::sync::mpsc;
use rpc_io::{Endpoint, Responder, Router};

fn main() -> Result<(), rpc_io::Error> {
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
    Ok(())
}
```

### Protocol presets

`Config::lsp()` and `Config::mcp()` set the cancellation spelling, the `initialize` gate, and the limits each protocol wants (MCP turns batches off, as its 2025-06-18 revision removed them, and serves `ping` before `initialize`). `Limits::network()` and `Limits::strict()` cut every budget for peers across a network. [`Progress`](./docs/API.md#progress-and-workdone) sends MCP's `notifications/progress` and [`WorkDone`](./docs/API.md#progress-and-workdone) LSP's `$/progress` for the token a request carries:

```rust
use rpc_io::{Config, Endpoint, Progress, Router};

fn main() -> Result<(), rpc_io::Error> {
    let router = Router::new()
        .request("initialize", |_: &mut (), _, _| Ok(jsonx_lib::json!({ "protocolVersion": "2025-06-18" })))
        .request("tools/call", |_, params, cx| {
            if let Some(progress) = Progress::from_params(&params, cx.peer()) {
                progress.report(1.0, Some(1.0), Some("done"))?;
            }
            Ok(jsonx_lib::json!({ "content": [] }))
        });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ()).with_config(Config::mcp());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#)?;
    endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"_meta":{"progressToken":"p"}}}"#)?;
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":"p","progress":1,"total":1,"message":"done"}}"#);
    Ok(())
}
```

### Async, and the lower layers

`Endpoint::next_read`, `commit`, and `finish` are `run` taken apart: the endpoint says where to read, any reader (blocking, non-blocking, or `.await`ed on any runtime) fills it, and the endpoint handles what arrived. With the `futures-io` feature, `Endpoint::run_async` and `Outbox::write_to` do this over `AsyncRead` and `AsyncWrite`. Below the endpoint, the decoder and `decode` work on their own:

```rust
use rpc_io::{decode, Framing, Incoming, Limits, Message};

fn main() -> Result<(), rpc_io::FrameError> {
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    decoder.feed(b"Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}");
    let body = decoder.next_frame()?.unwrap_or_default();
    let Incoming::Single(Ok(Message::Request(request))) = decode(body, &Limits::new()) else {
        panic!("expected a request");
    };
    assert_eq!((request.method.as_ref(), request.id.as_i64()), ("ping", Some(1)));
    Ok(())
}
```

Typed params through `serde`, the async adapter, the TCP server, and the interceptor have examples in [`docs/API.md`](./docs/API.md).

<hr>
<br>

## Examples

Runnable programs in [`examples/`](./examples):

| Example | What it shows |
|---|---|
| [`stdio_echo`](./examples/stdio_echo.rs) | A server on stdin and stdout, newline framing (MCP) or `--content-length` (LSP): `echo '{"jsonrpc":"2.0","id":1,"method":"ping"}' \| cargo run --example stdio_echo` |
| [`pipe_pair`](./examples/pipe_pair.rs) | A client and a server over an in-memory pipe, each on its own thread: concurrent requests correlated by id, a deferred answer with progress notifications, and a cancelled request. |
| [`tcp_server`](./examples/tcp_server.rs) | A `Server` on a TCP port with per-connection state, network limits, an interceptor, and a clean stop (`--features net`). |

<hr>
<br>

## Performance

Decoding is one pass of `jsonx-lib`'s pull reader over each body: the envelope members are read as raw slices, so `params` and `result` are validated once and never copied or decoded unless a handler asks. Method names and string ids borrow from the body when they have no escapes, and deferred requests are looked up without copying their ids. `Framed` frames messages into a reused buffer and writes once per turn, however many messages a handler sends; `Queued` writes each batch of queued messages with one `write_all` and one `flush`. The decoder reads straight into its own buffer, searches for line ends eight bytes at a time, resumes each search where the last one stopped, and grows geometrically while a large message arrives.

Measured with the benchmarks in [`benches/`](./benches), x86_64 Windows, Rust 1.99, release profile, on LSP-shaped `textDocument/hover` requests (about 160 bytes each). The machine was under heavy background load from other work, and repeated runs there differed by up to 40%, so these are lower bounds to compare trends with, not the crate's speed on a quiet machine. Across back-to-back runs of 0.2.0 and 0.3.0 there, neither was consistently faster: the differences between the versions were smaller than those between repeated runs.

| Benchmark | What it measures | Windows |
|---|---|---:|
| `framing/decode_100k_ContentLength` | Cut 100,000 Content-Length frames out of one 19 MiB stream. | ~2.7 GiB/s (6.8 ms) |
| `framing/decode_100k_Newline` | The same with newline framing. | ~6.1 GiB/s (2.7 ms) |
| `framing/encode_100k_*` | Frame 100,000 bodies (Content-Length / newline). | ~1.8 / ~1.4 GiB/s |
| `messages/decode_100k_requests` | Decode 100,000 requests: envelope, id, method, `params` validated. | ~1.6 M/s (~610 ns each) |
| `messages/decode_batch_1024` | Decode a full batch of 1,024 requests. | ~460 µs |
| `messages/decode_and_write_100k_responses` | Decode 100,000 responses and write each back. | ~1.6 M/s |
| `endpoint/handle_100k_requests` | The whole server path, 100,000 times: decode, route, parse `params`, build a result, encode, frame, write. | ~420 K/s (~2.4 µs each) |
| `endpoint/run_100k_framed_requests` | The same, read from one Content-Length stream by `run`, written by `Framed`. | ~455 K/s |
| `endpoint/run_100k_queued` | The same through a `Queued` sink: a writer thread does the writes, and the time includes waiting for it to finish. | ~355 K/s |
| `pending/issue_then_settle_100k_out_of_order` | Issue 100,000 requests with deadlines, settle them in scrambled order. | ~1.2 M pairs/s |

The queued path costs a hand-off to the writer thread per batch; it buys senders that do not wait on the peer while the connection is open. Use `Framed` for a peer that always reads (a local pipe), `Queued` for anything that might stall.

```bash
cargo bench --bench bench
```

Criterion writes per-benchmark reports to `target/criterion/`. Numbers vary by CPU and load; use the trend across runs, not a single absolute.

<hr>
<br>

## Design notes

- **Exactly one response.** A deferred request is tracked until answered; a `Responder` dropped without answering (a worker returned early, a panic unwound through it) answers `INTERNAL_ERROR` itself. A cancelled request is answered by the endpoint (`-32800` for LSP) or not at all (MCP), and a late answer from the worker returns `Error::AlreadyAnswered` instead of reaching the wire.
- **Ids.** Request ids are strings or integers within `i64`. `null`, fractions, and other values are refused as invalid requests, as LSP and MCP require. A request whose id is still being answered by a deferred handler is refused under a `null` id, so the only response carrying that id is the real answer (a deliberate departure from the letter of JSON-RPC 2.0, which keeps `null` for ids that could not be read). Our own ids are positive integers counted from 1; answers to ids we are not waiting on are reported and dropped.
- **Malformed messages.** A message with a `method` is answered under its own id when that id is valid and appears once; anything unreadable is answered under `null`, as the specification requires. A broken *response* is never answered (an error under its id would read as an answer to one of the peer's requests); it ends the call it names instead.
- **Budgets.** `Limits` caps message bytes (64 MiB), header bytes (8 KiB, the terminating blank line included), header lines (32), batch size (1024, or 0 for no batches), the length an oversized body may announce and still be skipped, and every JSON budget of `jsonx-lib` (depth 128 by default). An oversized message is skipped as it arrives, never buffered, and answered; a broken frame ends the connection, since no reliable boundary is left. The defaults suit a local peer; `Limits::network()` and `Limits::strict()` suit remote ones, since a handler that parses `params` builds a value tree that can be several times the message's size.
- **Output.** A `Sink` runs under the endpoint's lock. `Framed` writes in the sending thread, which is fine for a peer that always reads; with `Queued` and `outbox` a send does not wait on the peer: messages wait in a bounded queue (bytes, messages, and a no-progress write timeout), and a peer that stops reading closes the connection instead of freezing it. Closing takes the lock only to close the queue; the closing thread then waits for the drain without it, for at most `QueueLimits::close_timeout` (10 s), and other threads see the connection closed at once. Messages this side sends before the gate opens are held, within `Config::max_held_bytes`.
- **Calls.** Dropping a `Call` before its outcome cancels the request: its pending slot is freed and the peer is told, as a dropped future should (a `select!` that took another branch). `Call::detach` keeps a request outstanding without a waiter.
- **Deadlines.** A request's deadline fires on its own: a timer thread, started with the first deadline and stopped when the connection closes, ends the call with `TimedOut` and tells the peer to cancel, even when nothing else happens. `Config::with_timer(false)` leaves them to the caller.
- **Panics.** Handler, interceptor, and event-hook panics are caught; so are panics in a `Sink` or a `Queued` writer, which close the connection as an I/O error. This needs `panic = "unwind"`, Rust's default. An I/O error a handler forwards with `?` reaches the peer as "the transport failed", without the operating system's text.
- **Threads.** Handlers run on the endpoint's thread, one at a time, in arrival order. `Peer`, `Call`, `Responder`, and `Canceller` are `Send` and work from any thread. The endpoint's lock is held to encode a message and hand it to the sink, never while a handler or hook runs; with `Queued` or `outbox`, never during a write either. A message sent from a worker goes out at once, even while a handler runs.
- **Observability.** `Endpoint::stats()` and `Peer::stats()` give the connection's counters (traffic both ways, errors, timeouts, evictions, cancellations, panics) and gauges (pending, in flight, held). The `tracing` feature adds a span per call and events at documented levels. They record methods, ids, sizes, error codes, error messages, and panic messages, never `params`, results, or error `data` as such. Text the peer chose is clipped and escaped; an error message may quote part of what the peer sent (a `serde` error names the value that did not fit), clipped to 256 bytes.
- **Serving.** `Server` closes a connection that leaves a message unfinished for `message_timeout` (30 s, also the time a new connection has to send its first message) or stays silent between messages for `idle_timeout` (300 s), so silent or trickling sockets cannot hold its `max_connections` slots. Its defaults are for untrusted peers: `Limits::network()` (4 MiB messages) and an 8 MiB, 4,096-message queue per connection. Running out of descriptors or memory at `accept` pauses the loop (10 ms, doubling to 1 s) instead of ending it.

<hr>
<br>

## Testing

```bash
cargo test                       # unit + integration + property + doctests
cargo test --all-features        # with serde, futures-io, tracing, net
cargo test --no-default-features # the no_std + alloc layers
cargo clippy --all-targets --all-features -- -D warnings
cargo bench --bench bench
cargo +nightly fuzz run content_length   # also: newline, message, batch, endpoint
```

[`tests/properties.rs`](./tests/properties.rs) holds the crate to independent references: a classifier built on a full `jsonx-lib` parse decides what every random envelope should decode to, and random batches check one response per request id. [`tests/fuzz_harness.rs`](./tests/fuzz_harness.rs) runs the fuzz targets' checks on random, JSON-shaped, and mutated bytes in every `cargo test`. [`tests/endpoint.rs`](./tests/endpoint.rs) runs section 7 of the JSON-RPC 2.0 specification verbatim, [`tests/framing.rs`](./tests/framing.rs) runs the framings against malformed and hostile streams, [`tests/session.rs`](./tests/session.rs) covers cancellation, timeouts, the gate, a client and server over a pipe, and races, [`tests/output.rs`](./tests/output.rs) stalls the peer, and [`tests/adversarial.rs`](./tests/adversarial.rs) holds regression tests for the 0.2 review's findings; the 0.3 audits' regressions sit with the behaviour they cover (`tests/output.rs`, `tests/server.rs`, `tests/session.rs`, `tests/typed.rs`, `tests/tracing.rs`). Every `rust` example in this README and in [`docs/API.md`](./docs/API.md) is compiled and run as a doctest.

<hr>
<br>

## Cross-platform support

CI builds and tests on:

- Linux (x86_64), and 32-bit Linux (i686)
- macOS (Apple Silicon)
- Windows (x86_64)

and builds (without running) the `no_std` layers for bare metal (`thumbv7em-none-eabihf`).

The crate uses only `std`'s threads, channels, clocks, and sockets, and no platform-specific code beyond Unix domain sockets in `Server::serve_unix` and how `Server` waits for a socket to become readable on Windows (a short timed `peek`, since shutting a socket down there does not wake a blocked read). Other targets (aarch64 Linux, x86_64 macOS) are expected to work but are not exercised by CI.

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
