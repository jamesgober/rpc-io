# rpc-io - Roadmap

> Path from scaffold to a stable 1.0. Hard parts are front-loaded; each phase has hard exit criteria.
> Master plan: C:/Dev/libraries/Rust/lang-collection/_lexersketch/ROADMAP.md and NEW-LIBS.md
>
> **Anti-deferral rule:** no listed hard task moves to a later phase unless this file records the move and the reason.

## v0.1.0 - Scaffold (DONE)
Compiles, CI green, structure correct, no domain logic.
- [x] Manifest, README, CHANGELOG, REPS, dual license, CI, deny, clippy, rustfmt, DIRECTIVES, ROADMAP.

## v0.2.0 - Foundation
- [ ] Envelope model, Content-Length and newline framings over Read/Write, id correlation, cancellation tokens, batches; built on jsonx-lib.

## v0.5.0 - Implementation
- [ ] HTTP framing over the owner's net crates, async adapter, deferred replies, progress notifications.

## v0.9.0 - Hardening
- [ ] Fuzzing framings; conformance tests.

## v1.0.0 - Stable
- [ ] Frozen after lsp-lang and mcp-io run on it (D18).
