<h1 align="center">
    <img width="90px" height="auto" src="https://raw.githubusercontent.com/jamesgober/jamesgober/main/media/icons/hexagon-3.svg" alt="Triple Hexagon">
    <br><b>CHANGELOG</b>
</h1>
<p>
  All notable changes to <code>rpc-io</code> will be documented in this file. The format is based on <a href="https://keepachangelog.com/en/1.1.0/">Keep a Changelog</a>,
  and this project adheres to <a href="https://semver.org/spec/v2.0.0.html/">Semantic Versioning</a>.
</p>

---

## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [0.4.0] — 2026-10-19

Typed JSON through jsonx-lib 0.5, MCP as its current revision has it, and the 1.0 decisions. rpc-io moves onto jsonx-lib 0.5 (re-exported as `rpc_io::jsonx`) and makes its `ToJson`/`FromJson` traits the primary typed API, with results written into reused buffers; `Config::mcp()` follows MCP's stateless 2026-07-28 revision; `Progress` follows MCP's rules; `Notifier` binds notifications to their request; the message structs are `#[non_exhaustive]` and lenient ids are available behind a switch. Repeated envelope members were checked against the jsonx-lib audit's smuggling case: 0.3.0 was not affected. See [`docs/release/v0.4.0.md`](./docs/release/v0.4.0.md).

### Added

- `Request::new`, `Notification::new`, `Response::new`, `Response::ok`, and `Response::err`.
- `Config::mcp_legacy()`: the `initialize`-gated preset, for servers whose MCP clients all speak a legacy revision (2025-11-25 and earlier).
- `Config::omit_null_id` / `with_omit_null_id`: an error for a message whose id could not be read leaves `id` out instead of writing `"id": null` (MCP's schema has `id?: RequestId`, never `null`).
- `Config::overloaded_code` / `with_overloaded_code`: the code of the refusal past `max_in_flight` (default `-32000`, as before).
- **Lenient ids** (audit RPC-COR-06, decided for the 1.0 surface): `Limits::lenient_ids` / `with_lenient_ids(true)` accepts the request ids JSON-RPC 2.0 allows and discourages, `null` and numbers that are not integers within `i64`, and echoes them back exactly as sent, for generic JSON-RPC services. Off by default and in every preset (LSP and MCP forbid them). `Id::RawNumber`, `Id::Null`, `Id::raw_number`, `Id::is_null`.
- `Context::progress` and `Responder::progress`: a `Progress` bound to its request.
- `Error::ProgressNotIncreasing { last, next }`.
- **Typed results and params through jsonx-lib's `ToJson`**, the primary typed API (moved from the v0.2.0 roadmap; the `serde` feature stays as the alternative): `Router::request_encoded` writes a handler's `ToJson` result straight into a buffer the endpoint reuses, so no `Value` is built and, once the buffer has grown, nothing is allocated for the result; `Peer::notify_json`, `Peer::request_json`, `Context::notify_json`, `Context::request_json`, and `Responder::ok_json` encode in a per-thread reused buffer, outside the endpoint's lock; `Json::encode`; `ToJson` for `Id` and `ErrorCode`.
- **Typed params and results through jsonx-lib's `FromJson`**: `Router::request_json` (params `P: for<'de> FromJson<'de>`, result `R: ToJson`, written into the endpoint's reused buffer), `Router::notification_json`, `Params::decode` (with the endpoint's budgets and duplicate keys refused; borrows from the message where the type does), `Raw::decode`, `Call::wait_json`. Decode errors are `INVALID_PARAMS` naming the path and byte offset, clipped. `FromJson` for `Id`, `ErrorCode`, and `Raw`; `ToJson` for `ErrorObject`, `Raw`, and `Json`.
- `rpc_io::jsonx`: jsonx-lib re-exported, so a crate on top (an LSP or MCP server) builds and reads JSON, and implements `ToJson`/`FromJson`, through rpc-io with no dependency of its own and always with rpc-io's jsonx-lib version. The examples use it.
- `Notifier`, from `Responder::notifier` and `Context::notifier`: notifications that belong to one request (MCP 2026-07-28's `subscriptions/listen` stream), refused with `Error::AlreadyAnswered` once the request is answered or cancelled, checked under the lock the response is written under (requested by mcp-io); `notify` and `notify_json`.
- The `typed` benchmarks (`benches/typed.rs`): 100k hover requests through `FromJson` params borrowed from the message with a `ToJson` result (`request_encoded` + `Params::decode`), through `request_json`, and, with `serde`, through `serde`; and 100k progress reports.
- The `endpoint/handle_100k_unparsed` benchmark: the endpoint's own cost, with a handler that reads nothing.

### Changed

- **Breaking:** `Request`, `Notification`, and `Response` are `#[non_exhaustive]`, decided for the 1.0 surface (audit RPC-CQ-05), so a later version can keep more of the envelope without a break. Their fields stay public to read and to change. Migration: build them with the new constructors instead of struct literals, and add `..` to struct patterns.
- **Breaking:** `Id` has two more variants, `RawNumber(Cow<str>)` and `Null`, produced only with lenient ids. `Id` stays exhaustive, since these are every id JSON-RPC 2.0 allows. Migration: add arms (or `_`) to exhaustive `match`es on `Id`.
- `docs/API.md`'s examples have no rustdoc-hidden (`# `) lines, which GitHub shows verbatim: each is a whole program, still compiled and run by `cargo test` (0.3.0 release verification, Info).
- **Breaking:** built on jsonx-lib 0.5 (from 0.2), so this release is published after jsonx-lib 0.5.0 is on crates.io. jsonx-lib's types are part of this crate's API (`Raw::parse` returns a `jsonx_lib::Value`, `Json` converts from one, `Raw::new_with` takes a `jsonx_lib::Parser`), so a crate that names them needs the same jsonx-lib. jsonx-lib 0.5 keeps the 0.2 surface these use. Migration: depend on `jsonx-lib = "0.5"`.
- `Progress::report` and the `WorkDone` reports encode their params in a per-thread reused buffer instead of allocating them per report (one string per MCP report, two per LSP report in 0.3).
- The fuzz harness (and its checks in `cargo test`) covers lenient ids and errors written without `id`.
- The README's benchmarks are re-measured (0.3.0 release verification), as ranges, with the comparison against 0.3.0 counted in CPU cycles per request by an interleaved harness now in the repository (`dev/ab`). The result depends on the compiler: built with rustc 1.99, 0.4.0 is 2-12% cheaper on every path (progress reports about 76%); built with rustc 1.95, the endpoint path, `decode`, `run`, and the serde path cost 1.4-5.2% more than 0.3.0's, while handlers that parse params stay 4-5% cheaper and progress reports about 76% cheaper.
- `Raw` holds a jsonx-lib `RawValue` when it comes from a message (params, results, and error data), `Raw::new` or `new_with` (when the text is one value), `Raw::from_value`, `Json::encode`, or `FromJson for Raw`, validated once when it was read or written, so `ToJson for Raw` copies it with no second scan. `Raw::from_string`, `Raw::NULL`, progress tokens, and `serde` output hold plain text, validated again only if one is written into a `ToJson` value. Equality and hashing go by the text, as before.
- `Router::request_typed` (the `serde` feature) serializes its result into the buffer the endpoint reuses instead of a new string per request, which keeps the serde path at the cost it had in 0.3.0 (CPU cycles per request within -0.4% to +1.0% of 0.3.0's, measured interleaved; it was about 3.5% more without this).

### Fixed

- **Breaking (behaviour), MCP:** `Config::mcp()` now follows MCP's current revision, 2026-07-28, which is stateless. In 0.3 it gated every request on `initialize`, so a modern client (which never sends one) had every request but `ping` refused, with `-32002`, a code the revision forbids. It now has no gate, writes errors for unreadable messages without `id`, and refuses requests past `max_in_flight` with `-32603` instead of `-32000` (MCP asks new implementations not to use `-32000..=-32019`). Batches stay off and the cancellation spelling is unchanged. Migration: a server for legacy clients only uses `Config::mcp_legacy()`; a dual-era server keeps `Config::mcp()` and picks each request's semantics in the protocol layer, as the specification describes.
- **Breaking (behaviour):** `Gate::mcp()` refuses early requests with `-32600` instead of `-32002`, which legacy MCP used for "resource not found". Migration: `Gate::mcp().with_code(..)` for another code.
- **Breaking (behaviour):** an error response from the peer with no `id` member (MCP's form) is read as an error under `null` (reported as `Event::PeerError`) instead of being dropped as invalid (`Event::Invalid`). Migration: an `on_event` hook that counted these as invalid matches `Event::PeerError`.
- **Breaking (behaviour), MCP progress** (mcp-io audit MCP-ARC-05): `Progress::report` refuses a `progress` that does not exceed the last one sent for its request, by any of the request's handles (or, for a handle bound to no request, by it and its clones) (`Error::ProgressNotIncreasing`), as MCP requires that it increase with every notification; the check and the send share a lock, so concurrent reports reach the wire in order. A `Progress` from `Context::progress` or `Responder::progress` refuses to report (`Error::AlreadyAnswered`) once its request is answered or cancelled, as MCP requires that progress stop at completion and that nothing more be sent for a cancelled request; the check runs under the lock the response is written under. `Progress::new` and `from_params` keep working, bound to no request. Migration: nothing is sent when a report is refused; report increasing values, and treat the error as "not sent" (`.ok()` keeps a fire-and-forget call compiling and quiet).
- **Breaking (behaviour):** `Server`'s idle timeout (the 0.3.0 release verification's RPC4-001, the case its fix left open): a client could still hold a connection out of `idle_timeout` forever by sending a request whose deferred handler waits on a request of ours that has no timeout, and never answering that. While a request of ours without a timeout is outstanding, the peer's own requests in flight no longer count as work; a request of ours within its timeout still does. As a result, a client that takes longer than `idle_timeout` (300 s by default) to answer a request of ours that has no timeout (a slow MCP `sampling/createMessage`, for example) is now disconnected. Migration: give the request a timeout (`request_with_timeout`, `Config::with_request_timeout`), which keeps the connection busy until it passes, or raise or remove the limit with `Server::with_idle_timeout`.

### Security

- **A stale `Responder` answered a later request that reused its id. Affects 0.2.x and 0.3.0.** A deferred request's `Responder` was matched to the in-flight request by its id alone. When the peer cancelled a deferred request and then sent a new request with the same id (JSON-RPC and MCP allow reusing an id once the earlier request is settled), the worker still holding the old `Responder` answered the *new* request: its late `ok`/`err` went out as the new request's response (the result of a cancelled job delivered as the answer to an unrelated request), and the new request's own `Responder` got `Error::AlreadyAnswered`; dropping the old `Responder` instead answered the new request with `-32603` "the handler dropped the request without answering". Each deferred request now has a serial, unique on its connection, that its `Responder` carries: a stale `Responder` answers nothing (`Error::AlreadyAnswered`), and dropping one sends nothing. The request-bound `Notifier` and `Progress` added in this release are bound the same way (during development they came back to life when an id was reused). Regression tests in `tests/reuse.rs`, with a cancel-then-reuse race. Found by the 0.4.0 release verification (RPC5-008, RPC5-001).
- Repeated envelope members (the jsonx-lib baseline audit, JX-SEC-02: a pull-reader loop that keeps the last key lets `{"method":"initialize","method":"shutdown"}` be read as one method and run as another): rpc-io was **not affected, in 0.3.0 or before**. Its envelope reader records every repeated `jsonrpc`, `id`, `method`, `params`, `result`, or `error` and refuses the message with `-32600` (under no id when `id` is the repeated member); a repeated `code`, `message`, or `data` in an `error` makes the response invalid; a cancel target or progress token whose member repeats names nothing. New regression tests (`tests/duplicates.rs`) pin each case, through the gate and in batches, and pass unchanged on 0.3.0. jsonx-lib 0.5's readers refuse repeated keys under their default policy; rpc-io reads the envelope (and `Raw::new`, and the error object, cancel targets, and progress tokens) with repeats passed through, so its own envelope check decides, as in 0.3, and a repeated member inside `params` is still refused when the params are parsed (`-32602`).

---

## [0.3.0] — 2026-10-13

Hardening, and the adapters around the engine: non-blocking output, self-firing deadlines, an async adapter, typed params, observability, protocol presets, serving helpers, and fuzz targets for every parser of untrusted input. Fixes the findings of the 2026-10-13 audit and of its release verification, except six deferred or waived with reasons in `dev/ROADMAP.md` and `dev/DIRECTIVES.md` (RPC-COR-06, RPC-CQ-05, and RPC-STD-05 deferred; RPC-CQ-01, RPC-STD-03, and RPC-STD-04 waived). See [`docs/release/v0.3.0.md`](./docs/release/v0.3.0.md).

### Added

- **Non-blocking output:** `Queued`, a `Sink` that frames messages into a bounded queue and writes them from its own thread; `outbox()`, the same queue drained by an async task (`OutboxSink` for the endpoint, `Outbox` with `poll_chunk`/`consume` for the writer); `QueueLimits` (`max_bytes` 64 MiB, `max_messages` 65,536, `write_timeout` 60 s without progress, `close_timeout` 10 s). A provided `Sink::close`, called once when the endpoint closes or after a send or flush failed, and a provided `Sink::drain` returning a `Drain`: what the closing thread waits for after releasing the endpoint's lock.
- **Self-firing deadlines:** a timer thread per connection, started with the first deadline, ends due requests with `TimedOut` and tells the peer, with no traffic and no waiter; `Config::timer` / `with_timer`.
- **Async:** `Call` is a `Future`, and `Call::detach` lets a request run without a waiter. `Endpoint::next_read`, `commit`, and `finish` (`run` taken apart, with `Next`) for any reader. The `futures-io` feature: `Endpoint::run_async`, `Outbox` as `AsyncRead` and `AsyncBufRead`, and `Outbox::write_to`.
- **Typed params and results** (the `serde` feature): `Router::request_typed`, `Router::notification_typed`, `Params::deserialize`, `Raw::deserialize`, `Json::serialize`, `Peer::notify_typed`, `Peer::request_typed`, `Call::wait_typed`, `Responder::ok_typed`, `Serialize`/`Deserialize` for `Id` and `ErrorCode`; `Error::Serialize`; `Event::InvalidParams`.
- **Observability:** `Stats` from `Endpoint::stats` and `Peer::stats` (traffic both ways, errors, timeouts, evictions, cancellations, panics, and the pending, in-flight, and held gauges). The `tracing` feature: a span per call and events for what the endpoint does on its own, never `params`, results, or error `data`; text the peer chose is clipped and escaped; off, it compiles to nothing.
- **Interceptor:** `Router::intercept`, run before every routed call, for authorization, rate limits, or an audit log; `Event::Refused`.
- **Presets:** `Limits::lsp`, `mcp`, `network`, `strict`, and `without_batches` (`max_batch` 0 refuses any batch with "batches are not accepted", for MCP since 2025-06-18); `Config::lsp` and `Config::mcp`; `Gate::lsp` and `Gate::mcp`; `Gate::allow_request` and `allow_requests` (MCP's `ping` before `initialize`).
- **Progress:** `Progress` (MCP `notifications/progress`) and `WorkDone` (LSP `$/progress` begin, report, end); `Params::progress_token` and `Params::work_done_token`.
- **Serving** (the `net` feature): `Server`, an accept loop with one endpoint per connection over TCP and Unix domain sockets, bounded by `max_connections`, with a `Stopper` and `Connection`. Connections that leave a message unfinished (`with_message_timeout`, 30 s, also the time to send a first message) or stay idle, with no traffic either way and nothing in flight (`with_idle_timeout`, 300 s) are closed. Defaults for untrusted peers: `Limits::network()` and an 8 MiB, 4,096-message queue per connection. Running out of descriptors or memory at `accept` pauses the loop instead of ending it.
- `Endpoint::buffered_input`: the bytes read and held in the endpoint's decoder.
- `Endpoint::run_with`, to read with a decoder the caller owns.
- `Limits::max_skip_bytes` and `FrameError::TooLargeToSkip`: a Content-Length announcing more than can be skipped closes the connection.
- `Config::max_held_bytes` (4 MiB) and `Error::Backlog`: a budget on messages held behind the gate.
- `Decoder::with_read_chunk`, `read_chunk`, and `capacity`; `Framed::with_flush_threshold` and `flush_threshold`.
- `ErrorCode::CONTENT_MODIFIED`, `SERVER_CANCELLED`, and `REQUEST_FAILED` (LSP).
- Fuzz targets (`fuzz/`, cargo-fuzz) for Content-Length framing, newline framing, message decoding, batch decoding, and a live endpoint; their checks also run in `cargo test` on random, JSON-shaped, and mutated bytes (`tests/fuzz_harness.rs`).
- The `tcp_server` example; the `endpoint/run_100k_queued` benchmark; integration tests for the output path, async adapter, typed params, protocols, observability, the server, and the public surface.
- CI: no_std (`thumbv7em-none-eabihf`), 32-bit (`i686-unknown-linux-gnu`), fuzz smoke, semver, and coverage jobs; clippy for each optional feature alone.

### Changed

- `serve` writes through a `Queued` sink, from a writer thread, and returns once what is queued has been written (for at most `close_timeout`).
- Dropping a `Call` before its outcome cancels the request: its pending slot is freed and the peer is sent the cancel notification. `Call` is `#[must_use]`, so a request whose `Call` is discarded (`peer.request(..)?;`, which in 0.2 sent it and ignored the answer) warns at compile time; `Call::detach` keeps the 0.2 behaviour.
- Typed-param errors quote at most 256 bytes of `serde`'s message.
- A request reusing the id of one still being answered is refused under a `null` id (and reported as `Event::Invalid`), so the real answer stays the only response with that id.
- `From<Error> for ErrorObject` turns `Error::Io` into "the transport failed", without the operating system's text.
- `Endpoint::is_closed` and `Peer::is_closed` read a flag instead of taking the endpoint's lock.
- The endpoint keeps its decoder between `run` calls.
- The decoder reserves geometrically while a message grows, and its buffer, `Framed`'s, and the endpoint's encoding buffer shrink back after a large message.
- Deferred requests are looked up without copying their ids.
- `Router` notification handlers may refuse their params internally (for `notification_typed`); no public signature changed.
- Optional dependencies, each behind an off-by-default feature: `serde` and `serde_json` (`serde`), `futures-io` (`futures-io`), `tracing` (`tracing`). The default build still depends on `jsonx-lib` alone. Dev-dependency: `serde` with `derive`.
- The package leaves out `dev/`, `REPS.md`, `.github/`, and `fuzz/`.
- `rustfmt.toml` drops its nightly-only keys, which warned on every stable `cargo fmt`.

### Fixed

- Resuming `run` after a handler stopped the endpoint lost the messages already read ahead (audit RPC-COR-01): they are kept and handled first.
- Request deadlines did not fire while `run` was blocked reading (RPC-COR-03): the timer thread fires them.
- A request reusing an in-flight id got a second response under that id (RPC-COR-02).
- The blank line that ends a Content-Length header section counted against `max_header_bytes` when it arrived in pieces but not when it arrived whole, so the same stream was accepted or refused depending on read sizes. Found by the new fuzz harness; terminators now always count, as documented.
- Documentation: the engine does not speak DAP (only the Content-Length framing applies to it), and only `lsp-lang` runs on the crate today (RPC-DOC-01, RPC-DOC-02); `decode`'s "validated in full" now names the early batch refusals (RPC-COR-04); the README's lock claim, `next_deadline`, `End::Stopped`, the "no notification gets a response" claim, the platform list, and the performance numbers match the code; README examples render on GitHub without hidden lines; internal references are gone from the public docs (RPC-CQ-04).

### Security

- A peer that stops reading no longer stalls every thread that touches the endpoint (RPC-SEC-01): with `Queued` (which `serve` and `Server` use) or `outbox`, writes leave the endpoint's lock, and the connection closes once the queue's byte, message, or no-progress limit is reached. Closing waits for the queue to drain only after releasing the lock, and for at most `close_timeout`.
- `Server` closes silent and trickling connections, so they cannot hold its connection slots, and starts from network budgets.
- Peer-chosen text in `tracing` spans and events, and in typed-param errors, is clipped (and escaped in the log).
- Per-connection memory follows the current message, not the largest seen (RPC-SEC-02).
- `Limits::network` and `Limits::strict` for remote peers, and the memory amplification of `Params::parse` documented (RPC-SEC-03).
- OS error text no longer reaches the peer through `?` on an `Error::Io` (RPC-SEC-04).
- Messages held behind the gate are bounded (RPC-SEC-05).
- A huge announced Content-Length can close the connection instead of keeping it skipping (RPC-SEC-06).
- Fuzz targets for every parser of untrusted input (RPC-STD-01, RPC-TST-01).

---

## [0.2.0] - 2026-10-12

The foundation: a working JSON-RPC 2.0 engine with Content-Length and newline framings, built on `jsonx-lib`. See [`docs/release/v0.2.0.md`](./docs/release/v0.2.0.md).

### Added

- **Framing:** `Framing` (`ContentLength`, `Newline`) with `encode`; the sans-IO `Decoder` (`space`/`commit`, `feed`, `next_frame`, `finish`, `memory_bound`), bounded to one message plus `READ_CHUNK`; `FrameError`, where only `TooLarge` is recoverable.
- **Messages:** `decode` (one pass of `jsonx-lib`'s pull reader, `params` and `result` kept as borrowed slices), `Incoming`, `Entry`, `Message`, `Request`, `Notification`, `Response`, `Invalid` (with the reply to send), `write_batch`; `Id`, `Raw`, `Json`, `ErrorCode`, `ErrorObject`.
- **Correlation:** `Pending`, outstanding requests by id with deadlines and a capacity, every operation `O(log n)`.
- **Endpoint:** `Endpoint` (`handle`, `run`, `close`, `peer`, `with_config`, `state`, `with_sink`, `next_deadline`, `expire`), `serve`, `Flow`, `End`; `Router` with request, deferred, and notification handlers and an event hook; `Params`, `Context`, `Event`; `Peer`, `Call`, `Canceller`, `Responder`, `CancelToken`; `Config`, `Cancellation` (LSP and MCP presets), `Gate`, `Limits`; `Sink` and `Framed`; `Error`, `CallError`.
- Batches answered as one array, one response per request; deferred answers within a batch; cancellation both ways; an opening-request gate; handler, event-hook, and sink panics contained.
- `From<Error> for ErrorObject`, so `cx.notify(..)?` works in a request handler.
- Examples `stdio_echo` and `pipe_pair`; integration, property, and scale tests; criterion benchmarks at 100,000 messages.

### Changed

- `jsonx-lib` 0.2 wired as the only dependency; the `std` feature forwards to `jsonx-lib/std`.
- The release profile no longer sets `panic = "abort"`: containing handler panics needs unwinding.

### Fixed

- Adversarial review of the unreviewed 0.2 work in progress (each fix has a regression test in `tests/adversarial.rs`):
  - A Content-Length header line whose name is not an HTTP token (a body that ran into the next header, or a line starting with a space) was skipped as an unknown header, silently losing a message; it is now `FrameError::InvalidHeader`.
  - A repeated `id` behind another repeated envelope member was trusted, so the error went out under one of two conflicting ids; it now goes out under `null`.
  - An opening request cancelled under a cancellation that sends no reply (MCP) left the gate stuck half-open forever; the gate now shuts again.
  - A panicking `Sink` unwound through the endpoint (or through a worker thread); it now closes the connection as an I/O error.
  - A message sent from another thread while a handler ran waited for the handler to return before it was flushed; it now goes out at once.
  - An "answer" to one of our requests still held behind the gate settled it, though the peer had never seen it; it is now unmatched.
  - Newline framing judged a line split across reads by its length, but a line that arrived whole by its blankness first: an over-long whitespace-only line was skipped or refused depending on the read sizes, and an oversized line's `length` was `None` or `Some` the same way. Every line now counts against the limit, blank or not, and newline framing always reports `length: None`. Found by the chunking property on Linux; its seed is kept in `proptest-regressions/framing.txt`.
  - Looking up held requests scanned every held message, an `O(n·m)` path a peer could drive with forged answers; it is now `O(log n)`.
- The decoder did not compile (a moved range), and several examples dropped the endpoint they used.

---

## [0.1.0] - 2026-10-08

Initial scaffold and repository bootstrap. No domain logic yet &mdash; this release establishes the structure, tooling, and quality gates the implementation will be built on.

### Added

- `Cargo.toml` with crate metadata, Rust 2024 edition, MSRV 1.85.
- Dual `Apache-2.0 OR MIT` license files.
- `README.md`, `CHANGELOG.md`, and a documentation skeleton.
- `REPS.md` compliance baseline.
- `.github/workflows/ci.yml` CI matrix; `deny.toml`, `clippy.toml`, `rustfmt.toml`.
- `dev/DIRECTIVES.md` and `dev/ROADMAP.md` (committed engineering standards + plan).

[Unreleased]: https://github.com/jamesgober/rpc-io/compare/v0.4.0...HEAD
[0.4.0]: https://github.com/jamesgober/rpc-io/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/rpc-io/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/rpc-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/rpc-io/releases/tag/v0.1.0
