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

## v0.5.0 - Implementation
- [ ] HTTP framing over the owner's net crates, async adapter, progress notifications.
- [ ] Typed params and results through jsonx-lib's `ToJson`/`FromJson` (moved from v0.2.0: needs jsonx-lib 0.5, D25).
- [ ] A budget on messages held behind the gate (requests are bounded by `max_pending`; notifications held before the opening request are not).

## v0.9.0 - Hardening
- [ ] Fuzzing framings; conformance tests.

## v1.0.0 - Stable
- [ ] Frozen after lsp-lang and mcp-io run on it (D18).
