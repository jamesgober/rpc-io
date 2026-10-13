# rpc-io - Roadmap

> Path from scaffold to a stable 1.0. Hard parts are front-loaded; each phase has hard exit criteria.
> Master plan: C:/Dev/libraries/Rust/lang-collection/_lexersketch/ROADMAP.md and NEW-LIBS.md
>
> **Anti-deferral rule:** no listed hard task moves to a later phase unless this file records the move and the reason.

## v0.1.0 - Scaffold (DONE)
Compiles, CI green, structure correct, no domain logic.
- [x] Manifest, README, CHANGELOG, REPS, dual license, CI, deny, clippy, rustfmt, DIRECTIVES, ROADMAP.

## v0.2.0 - Foundation (DONE, 2026-10-12)
- [x] Envelope model, Content-Length and newline framings over Read/Write, id correlation, cancellation tokens, batches; built on jsonx-lib.

Delivered:
- Framing: `Framing` (`ContentLength`, `Newline`) with `encode`; a sans-IO
  `Decoder` bounded to one message plus `READ_CHUNK`, oversized messages
  skipped as they arrive, header names checked as RFC 9110 tokens, line-end
  search resumed (linear for any delivery pattern); `FrameError`.
- Messages: `decode` in one pass of jsonx-lib's pull reader (`params`,
  `result`, error `data` kept as borrowed `Raw` slices), `Message` /
  `Request` / `Notification` / `Response`, `Incoming` / `Entry`, `Invalid`
  with the reply to send, `write_batch`; `Id`, `Raw`, `Json`, `ErrorCode`,
  `ErrorObject`.
- Correlation: `Pending` (capacity with eviction, deadlines, `O(log n)`).
- Endpoint over Read/Write: `Endpoint` (`handle`, `run`), `serve`, `Router`
  (request, deferred, and notification handlers, event hook), `Params`,
  `Context`, `Event`, `Peer`, `Call`, `Canceller`, `Responder`,
  `CancelToken`, `Config`, `Cancellation` (LSP and MCP presets), `Gate`,
  `Limits`, `Sink`, `Framed`, `Error`, `CallError`.
- Policies held by tests: exactly one response per request and none per
  notification; JSON-RPC 2.0 error codes verbatim (section 7 of the spec is a
  test); handler, event-hook, and sink panics contained; cancellation races
  resolve to one outcome; no quadratic paths (tripwire tests).
- Adversarial review of the unreviewed work in progress: seven findings fixed,
  each with a failing test first (`tests/adversarial.rs`, CHANGELOG).

Moved, per the anti-deferral rule:
- **Deferred replies** (listed under v0.5.0) were delivered here: cancellation
  of the peer's requests needs a request that is still being answered, so
  `Router::deferred` and `Responder` came with cancellation tokens.
- **Typed params through jsonx-lib traits** moves to v0.5.0. jsonx-lib 0.2
  has no `ToJson`/`FromJson`; they arrive in jsonx-lib 0.5 (LexerSketch
  decision D25). Until then handlers take `jsonx_lib::Value` through
  `Params::parse` and return anything `Into<Json>`. Hard dependency: a
  jsonx-lib release this crate cannot make.

Dependency wiring (decided here):
- **jsonx-lib 0.2: wired**, the only dependency (`default-features = false`;
  the `std` feature forwards to `jsonx-lib/std`). Parsing, the pull reader's
  raw slices, string escaping, and `Value` in `Params::parse` and `Json`.
- No `-lang` crate is wired: method semantics belong to the protocol crates
  above (lsp-lang, dap-lang, mcp-io).
- Release profile: `panic = "abort"` removed, since containing handler panics
  (DIRECTIVES 4) needs unwinding.

## v0.3.0 - Hardening and adapters (DONE, 2026-10-13)
- [x] Every finding of the 2026-10-13 audit fixed (`_audits/2026-10-13/rpc-io.md`, outside the repo).

Delivered:
- Output: `Queued` (writer thread) and `outbox` (async-drained) behind
  `QueueLimits`; `serve` on `Queued`; `Sink::close`; lock-free `is_closed`.
  A peer that stops reading no longer stalls the endpoint (RPC-SEC-01).
- Deadlines fire on their own from a per-connection timer thread
  (RPC-COR-03); `Call` is a `Future`.
- The decoder lives in the endpoint (read-ahead kept after a stop,
  RPC-COR-01); `run_with`; `next_read`/`commit`/`finish`.
- Buffers shrink after a large message; geometric growth (RPC-SEC-02).
- Duplicate in-flight ids answered under `null` (RPC-COR-02); OS error text
  kept from the peer (RPC-SEC-04); `max_skip_bytes` (RPC-SEC-06).
- `Stats`, the `tracing` feature, `Router::intercept`.
- Presets: `Limits::{lsp, mcp, network, strict}`, `without_batches`,
  `Config::{lsp, mcp}`, `Gate::{lsp, mcp}`, `Gate::allow_request`.
- `Progress` (MCP) and `WorkDone` (LSP); LSP error codes.
- The `serde` feature (typed params and results), the `futures-io` feature
  (`run_async`, `Outbox::write_to`), the `net` feature (`Server` over TCP and
  Unix sockets).
- Fuzz targets for both framings, message and batch decoding, and a live
  endpoint, with their checks also in `cargo test`; the fuzz harness found
  and fixed a chunking dependence in the header budget.

Moved, per the anti-deferral rule:
- **Async adapter, progress notifications, and the held-message budget**
  (listed under v0.5.0) were delivered here: the audit made a non-blocking
  output path and self-firing deadlines urgent, and both are the async
  adapter's foundation; progress and the held budget are small and were
  audit findings or Should-adds.
- **Fuzzing** (listed under v0.9.0) was delivered here: DIRECTIVES §3.6
  requires a fuzz target for every decoder and parser, and the audit found
  the gap (RPC-STD-01).
- **Typed params through serde** arrived here as an optional feature, for
  interop with `lsp-types` and MCP schema crates. Typed params through
  jsonx-lib's own `ToJson`/`FromJson` stay at v0.5.0: they need jsonx-lib 0.5
  (D25), a release this crate cannot make.
- **DAP** is out of scope for the engine: DAP is not JSON-RPC. rpc-io's
  Content-Length framing serves dap-lang; DAP's envelope, correlation by
  `request_seq`, and its `cancel` request belong to dap-lang. The claim that
  rpc-io was "one transport for DAP" was corrected everywhere.

Deferred, with reasons:
- **Lenient ids** for generic JSON-RPC services (null and fractional ids
  echoed back, audit RPC-COR-06): needs a new `Id` representation, a
  breaking change; decided with the 1.0 surface.
- **`#[non_exhaustive]` on `Request`, `Notification`, `Response`, and
  `ErrorCode(pub i64)`** (RPC-CQ-05): breaking (struct literals); decided with
  the 1.0 surface.
- **loom models** of the cancel/answer/close races (RPC-STD-05): the shared
  state uses `std` channels, clocks, condvars with timeouts, and
  `catch_unwind`, which loom models only in part; the races stay tested
  statistically. Moves to v0.9.0.
- **Windows named-pipe serving**: a server needs overlapped I/O through
  Win32 FFI, which needs `unsafe` (forbidden) or a third-party dependency.
  Unix domain sockets are served; on Windows, any `Read + Write` pipe from
  another crate runs through `Endpoint::run` with a `Queued` sink. Moves to
  v0.5.0 with the HTTP framing work over the owner's net crates.
- **Streamable HTTP / SSE and WebSocket framings**: stay at v0.5.0, over the
  owner's net crates.
- **An `#[rpc]` proc-macro router**: after jsonx-lib 0.5 typed params; v0.5.0
  or later.
- **Detecting a `Sink` that calls back into the endpoint** (documented as
  forbidden): a reentrant lock would deadlock rather than report; detection
  needs a thread-id check on every handle call. Moves to v0.9.0.

## v0.5.0 - Implementation
- [ ] HTTP framing (Streamable HTTP, SSE) and WebSocket messages over the owner's net crates.
- [ ] Typed params and results through jsonx-lib's `ToJson`/`FromJson` (moved from v0.2.0: needs jsonx-lib 0.5, D25).
- [ ] Windows named-pipe serving (moved from v0.3.0, see above).
- [x] ~~Async adapter, progress notifications~~ (delivered in v0.3.0).
- [x] ~~A budget on messages held behind the gate~~ (delivered in v0.3.0).

## v0.9.0 - Hardening
- [x] ~~Fuzzing framings~~ (delivered in v0.3.0).
- [ ] Conformance suites: LSP base protocol and MCP JSON-RPC vectors, backed by the fuzz corpus.
- [ ] loom models of the cancel/answer/close races (moved from v0.3.0).
- [ ] Reentrant-sink detection (moved from v0.3.0).

## v1.0.0 - Stable
- [ ] Decide `#[non_exhaustive]` on the message structs and `ErrorCode`, and lenient ids (moved from v0.3.0).
- [ ] Frozen after lsp-lang and mcp-io run on it (D18).
