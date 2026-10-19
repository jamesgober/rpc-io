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
- [x] The findings of the 2026-10-13 audit fixed (`_audits/2026-10-13/rpc-io.md`, outside the repo), except the ones deferred below (RPC-COR-06, RPC-CQ-05, RPC-STD-05) and waived in `dev/DIRECTIVES.md` §5 (RPC-CQ-01, RPC-STD-03, RPC-STD-04).
- [x] The findings of the release verification fixed (`_audits/2026-10-13-release/rpc-io.md`, outside the repo).

Delivered:
- Output: `Queued` (writer thread) and `outbox` (async-drained) behind
  `QueueLimits`; `serve` on `Queued`; `Sink::close`; lock-free `is_closed`.
  A peer that stops reading no longer stalls the endpoint (RPC-SEC-01), and
  closing waits for the drain outside the lock, bounded by
  `QueueLimits::close_timeout` (`Sink::drain`, `Drain`).
- Deadlines fire on their own from a per-connection timer thread
  (RPC-COR-03); `Call` is a `Future`, cancelled when dropped before its
  outcome (`Call::detach` keeps it outstanding).
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
  Unix sockets, with message and idle deadlines, network defaults, and
  accept backoff).
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
  Content-Length framing can cut DAP messages (dap-lang does not use rpc-io
  today); DAP's envelope, correlation by `request_seq`, and its `cancel`
  request belong to dap-lang. The claim that
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
- **A shared timer and fewer threads per connection** (release verification
  RPC2-014): `Server` costs a reader, a writer, and (with deadlines) a timer
  thread per connection, which is documented. A shared timer wheel and a
  readiness-based server need a design of their own; v0.5.0, with the HTTP
  framings, which need the same.

## v0.4.0 - Typed JSON, MCP 2026-07-28, and the 1.0 decisions (DONE, 2026-10-19; published once jsonx-lib 0.5 is)
- [x] jsonx-lib 0.5 (from 0.2).
- [x] Typed params and results through jsonx-lib's `ToJson`/`FromJson` (moved here from v0.5.0, see below).
- [x] MCP's current revision, 2026-07-28: `Config::mcp()` stateless (no `initialize` gate, no `-32002`, no `-32000..=-32019` codes, errors for unreadable messages without `id`); `Config::mcp_legacy()` for legacy-only servers; id-less error responses read as errors under `null`.
- [x] MCP progress rules: `Progress` values strictly increase; a `Progress` bound to its request (`Context::progress`, `Responder::progress`) stops when the request is answered or cancelled.
- [x] The 1.0 decisions (moved here from v1.0.0, see below): `#[non_exhaustive]` on `Request`, `Notification`, `Response` with constructors; `ErrorCode` kept as a plain newtype; lenient ids behind `Limits::lenient_ids`.
- [x] The 0.3.0 release verification's non-blocking leftovers: `docs/API.md` examples without hidden lines; benchmarks re-measured.
- [x] mcp-io's request: `Notifier`, notifications bound to their request (MCP's `subscriptions/listen`).
- [x] Repeated envelope members (jsonx-lib audit JX-SEC-02): rpc-io was not affected; regression tests, and envelopes read with repeats passed through to rpc-io's own check under jsonx-lib 0.5's stricter default.

Delivered:
- `Router::request_json`, `request_encoded`, `notification_json`;
  `Params::decode` (borrowing from the message), `Raw::decode`,
  `Call::wait_json`; `Peer`/`Context`/`Notifier` `notify_json`,
  `request_json`; `Responder::ok_json`; `Json::encode`; `ToJson`/`FromJson`
  for the crate's values. The serde feature stays as the alternative.
- jsonx-lib 0.5's writer adopted where it removes allocations: typed results
  are written into a buffer the endpoint reuses (no `Value`, no per-request
  string), typed params, deferred answers, and progress reports into a
  per-thread buffer.
- `rpc_io::jsonx`, the re-export, so crates on top share rpc-io's
  jsonx-lib version.
- `Config::mcp()` for MCP 2026-07-28, `Config::mcp_legacy()`,
  `Config::omit_null_id`, `Config::overloaded_code`, `Gate::mcp()` on
  `-32600`; id-less error responses accepted.
- `Progress` strictly increasing and bound to its request
  (`Context::progress`, `Responder::progress`); `Notifier`.
- `#[non_exhaustive]` message structs with constructors; `Id::RawNumber`,
  `Id::Null`, `Limits::lenient_ids`.

Not done here, with reasons:
- **Resetting a `Call`'s deadline on a matching progress notification**
  (MCP "MAY"): not asked for by mcp-io; needs correlating progress tokens to
  our requests. Later, on request.
- **`ToJson` for every `Raw` without a second validation**: a `Raw` holds a
  `jsonx_lib::RawValue` (validated once when read or written by jsonx-lib)
  wherever users obtain one, and is written with no second scan. Text the
  crate writes outside jsonx-lib (message buffers, `serde` output) stays plain
  text and is validated if it is ever nested in a `ToJson` value: jsonx-lib
  declined an unchecked raw write, since its writer's output must always
  parse back.

Moved, per the anti-deferral rule:
- **Typed params through jsonx-lib's traits** (listed under v0.5.0, moved
  there from v0.2.0) were delivered here: jsonx-lib 0.5 brings `ToJson` /
  `FromJson`, and mcp-io 0.2 (D18) builds on this release.
- **The 1.0 decisions** (listed under v1.0.0, moved there from v0.3.0) were
  made here, so that 1.0 is a freeze: a 0.x minor may break, a 1.x may not.
  `Request`, `Notification`, and `Response` are `#[non_exhaustive]` (a later
  version can keep more of the envelope). `ErrorCode(pub i64)` stays a plain,
  exhaustive newtype: it is the JSON integer and nothing more, and the tuple
  constructor is how protocols spell their codes. `Id` gains `RawNumber` and
  `Null`, produced only with `Limits::lenient_ids` (off in every preset), and
  stays exhaustive: those are every id JSON-RPC 2.0 allows.
- **MCP's stateless revision** was not on this roadmap: MCP 2026-07-28
  removed the `initialize` handshake that 0.3's `Config::mcp()` gated on, so
  the preset refused every modern client; found by the mcp-io baseline audit
  (MCP-ARC-02, MCP-ARC-05).

Out of scope for this release (owner's decision for this round):
- **An `#[rpc]` proc-macro router**: stays after typed params; v0.5.0 or
  later. A proc-macro needs `syn`/`quote`, which "ours, not rented" rules
  out of the default build; `Router::request_json` covers typed routing by
  hand.

## v0.5.0 - Implementation
- [ ] HTTP framing (Streamable HTTP, SSE) and WebSocket messages over the owner's net crates.
- [x] ~~Typed params and results through jsonx-lib's `ToJson`/`FromJson`~~ (delivered in v0.4.0).
- [ ] Windows named-pipe serving (moved from v0.3.0, see above).
- [ ] A shared timer wheel and fewer threads per `Server` connection (moved from v0.3.0, see above).
- [x] ~~Async adapter, progress notifications~~ (delivered in v0.3.0).
- [x] ~~A budget on messages held behind the gate~~ (delivered in v0.3.0).

## v0.9.0 - Hardening
- [x] ~~Fuzzing framings~~ (delivered in v0.3.0).
- [ ] Conformance suites: LSP base protocol and MCP JSON-RPC vectors, backed by the fuzz corpus.
- [ ] loom models of the cancel/answer/close races (moved from v0.3.0).
- [ ] Reentrant-sink detection (moved from v0.3.0).

## v1.0.0 - Stable
- [x] ~~Decide `#[non_exhaustive]` on the message structs and `ErrorCode`, and lenient ids~~ (decided in v0.4.0).
- [ ] Frozen after lsp-lang and mcp-io run on it (D18).
