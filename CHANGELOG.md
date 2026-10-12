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

[Unreleased]: https://github.com/jamesgober/rpc-io/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jamesgober/rpc-io/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/rpc-io/releases/tag/v0.1.0
