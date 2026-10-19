# rpc-io &mdash; API Reference

> Complete reference for every public item in `rpc-io`; each section has an example.
> **Status: pre-1.0 (0.4.0).** The surface is refined across the 0.x series and
> frozen at `1.0.0`, after lsp-lang and mcp-io have run on it (LexerSketch
> decision D18). See [`../dev/ROADMAP.md`](../dev/ROADMAP.md).

<sub>Copyright &copy; 2026 <strong>James Gober</strong>.</sub>

## Table of contents

- [Overview](#overview)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Concepts](#concepts)
  - [The endpoint's contract](#the-endpoints-contract)
  - [Error codes](#error-codes)
  - [Ids](#ids)
  - [Batches](#batches)
  - [Cancellation](#cancellation-both-ways)
  - [Deadlines](#deadlines)
  - [Output and slow peers](#output-and-slow-peers)
  - [Threads, turns, and panics](#threads-turns-and-panics)
- [`serve`](#serve)
- [`Server`](#server)
- [`Endpoint`](#endpoint)
- [`Flow`, `End`, and `Next`](#flow-end-and-next)
- [`Router`](#router)
- [`Params`](#params)
- [`Context`](#context)
- [`Event`](#event)
- [`Stats`](#stats)
- [`Peer`](#peer)
- [`Call`](#call)
- [`Canceller`](#canceller)
- [`Responder`](#responder)
- [`Notifier`](#notifier)
- [`CancelToken`](#canceltoken)
- [`Progress` and `WorkDone`](#progress-and-workdone)
- [`Config`](#config)
- [`Cancellation`](#cancellation)
- [`Gate`](#gate)
- [`Limits`](#limits)
- [`Sink` and `Framed`](#sink-and-framed)
- [`Queued` and `Outbox`](#queued-and-outbox)
- [Typed params and results (`ToJson` / `FromJson`)](#typed-params-and-results-tojson--fromjson)
- [Typed params and results (`serde`)](#typed-params-and-results-serde)
- [The async adapter (`futures-io`)](#the-async-adapter-futures-io)
- [`Framing`](#framing)
- [`Decoder`](#decoder)
- [`FrameError`](#frameerror)
- [`decode`](#decode)
- [`Incoming` and `Entry`](#incoming-and-entry)
- [`Message`](#message)
- [`Request`, `Notification`, `Response`](#request-notification-response)
- [`Invalid`](#invalid)
- [`write_batch`](#write_batch)
- [`Id`](#id)
- [`Raw`](#raw)
- [`Json`](#json)
- [`ErrorCode`](#errorcode)
- [`ErrorObject`](#errorobject)
- [`Pending`](#pending)
- [`Error`](#error)
- [`CallError`](#callerror)
- [Feature flags](#feature-flags)

## Overview

| Item | Kind | Purpose |
|---|---|---|
| [`serve`](#serve) | fn | Serve a `Router` over a byte stream until it ends. |
| [`Server`](#server), `Stopper`, `Connection` | structs | An accept loop over TCP and Unix sockets, one endpoint per connection (`net`). |
| [`Endpoint`](#endpoint) | struct | One connection, both directions: dispatch, answers, correlation. |
| [`Flow`, `End`, `Next`](#flow-end-and-next) | enums | Whether to go on after a message; why `run` returned; what `next_read` needs. |
| [`Router`](#router) | struct | Request, deferred, and notification handlers, the interceptor, and the event hook. |
| [`Params`](#params) | struct | A call's `params`, parsed on demand. |
| [`Context`](#context) | struct | What a handler can reach: method, id, the peer, `stop`. |
| [`Event`](#event) | enum | What the endpoint handled on its own. |
| [`Stats`](#stats) | struct | A connection's counters and gauges. |
| [`Peer`](#peer) | struct | Send notifications and requests, from any thread. |
| [`Call`](#call), [`Canceller`](#canceller) | structs | One of our requests: await, poll, cancel. |
| [`Responder`](#responder), [`CancelToken`](#canceltoken) | structs | Answer a deferred request once; learn it was cancelled. |
| [`Notifier`](#notifier) | struct | Notifications sent only while their request is in flight. |
| [`Progress`, `WorkDone`](#progress-and-workdone) | structs | MCP `notifications/progress`; LSP `$/progress`. |
| [`Config`](#config), [`Cancellation`](#cancellation), [`Gate`](#gate), [`Limits`](#limits) | structs | Settings, presets, and budgets. |
| [`Sink`](#sink-and-framed), [`Framed`](#sink-and-framed), [`Drain`](#sink-and-framed) | trait, structs | Where outgoing messages go; what closing waits for. |
| [`Queued`, `outbox`, `Outbox`, `OutboxSink`, `QueueLimits`](#queued-and-outbox) | structs, fn | Outgoing queues whose senders do not wait on the peer. |
| [`Framing`](#framing), [`Decoder`](#decoder), [`FrameError`](#frameerror), `READ_CHUNK` | enum, struct, enum, const | Cut bodies out of a byte stream and frame them back. |
| [`decode`](#decode), [`Incoming`, `Entry`](#incoming-and-entry), [`Message`](#message), [`Request`, `Notification`, `Response`](#request-notification-response), [`Invalid`](#invalid), [`write_batch`](#write_batch) | the message model | Read and write JSON-RPC 2.0 messages. |
| [`Id`](#id), [`Raw`](#raw), [`Json`](#json), [`ErrorCode`](#errorcode), [`ErrorObject`](#errorobject) | values | Ids, JSON text, JSON to send, error codes and objects. |
| [`Pending`](#pending) | struct | Outstanding requests by id, with deadlines and a capacity. |
| [`Error`](#error), [`CallError`](#callerror) | enums | Why the connection, or one of our requests, failed. |

## Installation

```toml
[dependencies]
rpc-io = "0.4"   # JSON values through `rpc_io::jsonx` (jsonx-lib, re-exported)

# Framing, messages, and correlation only, without std (alloc only):
# rpc-io = { version = "0.4", default-features = false }

# Integrations, each off by default:
# rpc-io = { version = "0.4", features = ["serde", "futures-io", "tracing", "net"] }
```

## Quick start

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{End, Framing, Router};

    let router = Router::new()
        .request("ping", |_, _, _| Ok("pong"))
        .request("sum", |_, params, _| {
            let p = params.parse()?;
            Ok(p.as_array().map_or(0, |items| items.iter().filter_map(|v| v.as_i64()).sum::<i64>()))
        })
        .notification("exit", |_, _, cx| cx.stop());

    let mut input = Vec::new();
    for body in [r#"{"jsonrpc":"2.0","id":1,"method":"sum","params":[1,2,3]}"#, r#"{"jsonrpc":"2.0","method":"exit"}"#] {
        Framing::ContentLength.encode(body.as_bytes(), &mut input);
    }
    let end = rpc_io::serve(input.as_slice(), std::io::sink(), Framing::ContentLength, router)?;
    assert_eq!(end, End::Stopped);
    Ok(())
}
```

## Concepts

### The endpoint's contract

For any input, an [`Endpoint`](#endpoint):

- answers every request exactly once, and never answers a valid notification;
- answers a malformed message under its own id when the message has a `method` and a valid, unrepeated id, and under `null` otherwise (with no `id` member at all under `Config::omit_null_id`, as MCP's schema has it); never answers a malformed *response* (it ends the call it names with `CallError::InvalidResponse` instead);
- matches answers to our requests by id, and reports answers to ids it is not waiting on as [`Event::Unmatched`](#event);
- contains handler and interceptor panics: the request is answered with `INTERNAL_ERROR`, the session goes on;
- never panics, and holds memory within [`Limits`](#limits), [`QueueLimits`](#queued-and-outbox), and `Config::max_held_bytes`.

### Error codes

| Code | Constant | When the endpoint answers with it |
|---:|---|---|
| `-32700` | `ErrorCode::PARSE_ERROR` | The body is not valid JSON (including invalid UTF-8), or a batch has a syntax error before its limit. Always under `null`. |
| `-32600` | `ErrorCode::INVALID_REQUEST` | Valid JSON but not a valid message: wrong `jsonrpc`, non-string `method`, non-structured `params`, a bad id, a repeated envelope member, an empty or oversized batch, any batch while batches are off, a message over a JSON budget or over `max_message_bytes`, a repeated opening request, an id already in flight (answered under `null`, so the in-flight request's answer stays the only response with that id). |
| `-32601` | `ErrorCode::METHOD_NOT_FOUND` | No request handler for the method. |
| `-32602` | `ErrorCode::INVALID_PARAMS` | Returned by handlers; `Params::parse` and `Params::deserialize` produce it for params that do not parse or fit. |
| `-32603` | `ErrorCode::INTERNAL_ERROR` | A handler or the interceptor panicked, a `Responder` was dropped without answering, or a typed result could not be serialized. |
| `-32000` | `ErrorCode::SERVER_OVERLOADED` | More deferred requests than `Config::max_in_flight` (`Config::overloaded_code`; this is the default, and `Config::mcp()` uses `-32603`, since MCP asks new implementations not to use `-32000..=-32019`). |
| `-32002` | `ErrorCode::SERVER_NOT_INITIALIZED` | A request before the [`Gate`](#gate) opened (the gate's code; this is the default of `Gate::new`, and `Gate::lsp()`'s; `Gate::mcp()` uses `-32600`). |
| `-32800` | `ErrorCode::REQUEST_CANCELLED` | The peer cancelled the request (LSP; the cancellation's code). |
| `-32801`, `-32802`, `-32803` | `CONTENT_MODIFIED`, `SERVER_CANCELLED`, `REQUEST_FAILED` | LSP's codes, for handlers to answer with. |

### Ids

Request ids are strings or integers within `i64`. JSON-RPC 2.0 also allows `null` and fractions, and discourages both; LSP and MCP forbid them, and by default so does this crate: such a request is refused with `-32600` under `null`. Ids compare by value (`7` and `"7"` differ; `-0` reads as `0`, and is echoed as `0`). Our own requests are numbered from 1 upward.

With `Limits::with_lenient_ids(true)` (for generic JSON-RPC services) such requests are accepted: a `null` id is `Id::Null`, and any other number (a fraction, an exponent, an integer outside `i64`) is `Id::RawNumber`, kept as its exact text and echoed back as it was sent (`1.0` and `1.00` are different ids). A response from the peer may then carry a `RawNumber` id too; it matches none of ours. A `null` id names no one request: two `null`-id requests cannot both be in flight (the second is refused as a duplicate), and a cancel notification naming `null` cancels nothing.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Config, Endpoint, Limits, Router};

    let router = Router::new().request("m", |_: &mut (), _, _| Ok(true));
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let config = Config::new().with_limits(Limits::new().with_lenient_ids(true));
    let mut endpoint = Endpoint::new(tx, router, ()).with_config(config);
    endpoint.handle(br#"{"jsonrpc":"2.0","id":2.50,"method":"m"}"#)?;
    assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":2.50,"result":true}"#));
    Ok(())
}
```

A request that reuses the id of a request still being answered is refused with `-32600` under `null`. This deliberately departs from the letter of JSON-RPC 2.0 §5, which says a response's id must be the request's and keeps `null` for ids that could not be read: answering under the reused id would give the peer two responses with one id, one of them for a request it cannot tell apart. The in-flight request's answer stays the only response with that id.

### Batches

A batch is answered with one array holding one response per request, in the order they were answered, and nothing at all when it holds only notifications and responses. An empty batch, a batch over `Limits::max_batch`, and a batch with a JSON syntax error are each answered with a single error object, not an array. A batch over the limit is refused as soon as it passes the limit, without reading the rest (so its size cannot drive the work done), and with batches off (`max_batch` 0, as MCP's 2025-06-18 revision wants) any batch is refused at its `[`. A batch with deferred requests is sent when its last answer arrives. The [gate](#gate)'s opening request cannot be sent in a batch.

### Cancellation, both ways

[`Cancellation`](#cancellation) says how the protocol spells it. When the peer cancels a deferred request, the request's [`CancelToken`](#canceltoken) is set and the request is answered with the cancellation's code (LSP) or not at all (MCP); a later answer returns `Error::AlreadyAnswered`. When one of our requests is cancelled, times out, or is evicted, the peer is sent the cancel notification. Every race between an answer and a cancellation resolves to one outcome under the endpoint's lock.

### Deadlines

A request gets a deadline from `Peer::request_with_timeout` or `Config::request_timeout`. Deadlines fire on their own: the first request with one starts a timer thread for the connection, which sleeps until the earliest is due, ends it with `CallError::TimedOut`, and sends the peer the cancel notification, even when nothing arrives and nobody waits. The thread holds only a weak reference to the connection and stops when it closes. With `Config::with_timer(false)` there is no thread, and a deadline is enforced when it is next looked at: when the endpoint handles a message, at the next request, in `Call::wait`/`try_wait`, when an awaited `Call` is polled, or by `Endpoint::expire`.

### Output and slow peers

Every outgoing message goes to the endpoint's [`Sink`](#sink-and-framed), with the endpoint's lock held. A sink that blocks (`Framed` writing to a peer that has stopped reading) blocks every sender for as long, the endpoint's own thread included. With [`Queued`](#queued-and-outbox) and [`outbox`](#queued-and-outbox) a send does not wait on the peer: each message is framed into a bounded queue, and a writer thread, or an async task, does the writes. When the peer falls behind past [`QueueLimits`](#queued-and-outbox) (by default 64 MiB or 65,536 messages queued, or a writer that has written nothing for 60 seconds, checked at each send, flush, and close), the queue fails and the connection closes. Closing closes the queue under the lock without waiting; the closing thread then waits for what is queued to be written with the lock released, for at most `close_timeout` (10 s), so other threads see the connection closed at once. [`serve`](#serve) and [`Server`](#server) use `Queued`.

### Threads, turns, and panics

Handlers run on the thread that calls `handle` or `run`, one at a time, in arrival order; that call is a *turn*, and what handlers send during it is flushed once at its end. `Peer`, `Call`, `Canceller`, and `Responder` are `Send` and work from any thread; what they send from another thread goes out at once. The endpoint's lock is held to encode a message and hand it to the sink, never while a handler, the interceptor, or the event hook runs, so handles never deadlock against a handler. A `Sink` must not call back into the endpoint or its handles. Handler, interceptor, event-hook, sink, and writer panics are caught (`panic = "unwind"`, Rust's default, is required).

## `serve`

```rust,ignore
pub fn serve<R: Read, W: Write + Send + 'static>(input: R, output: W, framing: Framing, router: Router<()>) -> Result<End, Error>
```

The one-call server: reads framed messages from `input`, answers them through `output` with the same framing, and returns when the input ends (`End::Eof`) or a handler calls `Context::stop` (`End::Stopped`). Default [`Config`](#config). Output goes through a [`Queued`](#queued-and-outbox) sink with the default limits, and `serve` returns once what is queued has been written, or after `close_timeout` (10 s), or once the writes stop making progress. For state, settings, or sending to the peer, build an [`Endpoint`](#endpoint).

**Errors:** as [`Endpoint::run`](#endpointrun); `Error::Io` if the writer thread cannot start.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{End, Framing, Router};

    let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    let router = Router::new().request("ping", |_, _, _| Ok("pong"));
    assert_eq!(rpc_io::serve(input, std::io::sink(), Framing::Newline, router)?, End::Eof);
    Ok(())
}
```

## `Server`

With the `net` feature. An accept loop: one [`Endpoint`](#endpoint) per connection, each on its own thread, with output through a `Queued` sink.

| Method | Description |
|---|---|
| `new(framing) -> Self` | Defaults for untrusted peers: `Config::new()` with `Limits::network()` (4 MiB messages), `QueueLimits::new()` with 8 MiB and 4,096 messages, 256 connections, a 30 s message timeout, a 300 s idle timeout. |
| `with_config(self, config)`, `with_queue(self, limits)`, `with_max_connections(self, n)` | Settings for every connection; `max_connections` is at least 1. |
| `with_message_timeout(self, Option<Duration>)`, `with_idle_timeout(self, Option<Duration>)` | How long a message may take to arrive once its first bytes have (and a new connection to send its first message); how long a connection may stay idle: no traffic either way, no deferred request of the peer's being answered, and no request of ours awaiting the peer within its timeout (one sent without a timeout does not count, so a peer cannot hold a slot by never answering; long-lived LSP or MCP sessions may want `None`). `None` is no limit. A connection past either is closed. |
| `config(&self) -> &Config`, `queue(&self) -> QueueLimits`, `message_timeout(&self)`, `idle_timeout(&self)` | The settings. |
| `max_connections(&self)`, `connections(&self)` | The bound, and connections served now. |
| `stopper(&self) -> Stopper` | A `Clone + Send + Sync` handle: `stop()`, `is_stopped()`. |
| `serve_tcp(&self, TcpListener, connect)` | Serves until stopped; `connect: FnMut(&Connection) -> (Router<S>, S)` runs once per connection. |
| `serve_unix(&self, UnixListener, connect)` | The same over a Unix domain socket (Unix only). |

At `max_connections` the loop stops accepting (the system queues further connections) until one ends. A connection that leaves a message unfinished past `message_timeout` (a per-message clock, started by the read that brings the message's first bytes and restarted when it completes, so back-to-back streaming is never cut off), or stays idle past `idle_timeout`, is closed, so silent or trickling sockets delay other clients by at most `message_timeout` and cannot lock them out. A read that times out closes the connection; the socket is not read again. Each connection's socket gets the queue's `write_timeout` as its write timeout, and a failed write shuts the connection down. A connection costs a reader thread, a writer thread, and, once a request with a deadline is sent on it, a timer thread. `Stopper::stop` stops accepting, shuts every open connection down (the peers see the stream end), and the serve call returns once every connection's thread has finished. `Connection` (`#[non_exhaustive]`) has `id` (counted from 0) and `addr`; the example below uses it, and the `Stopper`. **Errors:** the I/O error that ended the accept loop. Aborted and reset connections are skipped; running out of descriptors, buffers, or memory pauses the loop (10 ms, doubling to 1 s), and it carries on.

```rust
#[cfg(not(feature = "net"))] fn main() {}
#[cfg(feature = "net")] fn main() -> Result<(), Box<dyn std::error::Error>> {
    use std::{io::{BufRead, BufReader, Write}, net::{TcpListener, TcpStream}};
    use rpc_io::{Framing, Router, Server};

    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = Server::new(Framing::Newline).with_max_connections(16);
    let stopper = server.stopper();
    let serving = std::thread::spawn(move || {
        server.serve_tcp(listener, |connection| {
            (Router::new().request("whoami", |id: &mut u64, _, _| Ok(*id)), connection.id)
        })
    });
    let mut client = TcpStream::connect(addr)?;
    client.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"whoami\"}\n")?;
    let mut line = String::new();
    BufReader::new(&client).read_line(&mut line)?;
    assert_eq!(line, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":0}\n");
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
    Ok(())
}
```

## `Endpoint`

```rust,ignore
pub struct Endpoint<S, K: Sink>
```

One JSON-RPC connection: a [`Router`](#router), the state `S` its handlers work on, and a [`Sink`](#sink-and-framed) for outgoing messages. `Send` when `S` is. Dropping it closes it.

| Method | Description |
|---|---|
| `new(sink, router, state) -> Self` | An endpoint with the default `Config`. |
| `with_config(self, config) -> Self` | Applies a [`Config`](#config); call before any traffic. |
| `peer(&self) -> Peer` | A sending handle, for any thread. |
| `state(&self) -> &S`, `state_mut(&mut self) -> &mut S` | The handlers' state. |
| `with_sink(&self, f) -> R` | Runs `f` on the sink under the lock (to inspect a `Framed<Vec<u8>>`). |
| `handle(&mut self, body: &[u8]) -> Result<Flow, Error>` | Handles one unframed body (a message or a batch). |
| `run(&mut self, input: impl Read, framing) -> Result<End, Error>` | Reads and handles framed messages until the input ends or a handler stops. |
| `run_with(&mut self, decoder: &mut Decoder, input) -> Result<End, Error>` | As `run`, with a decoder the caller owns (stream hand-off). |
| `next_read(&mut self, framing) -> Result<Next, Error>`, `commit(&mut self, n)`, `finish(&mut self) -> Result<End, Error>` | `run` taken apart, for async and non-blocking readers: handle what is buffered, read into the returned buffer, commit the count. |
| `run_async(&mut self, input: impl AsyncRead + Unpin, framing)` | `run` for an async stream (`futures-io`; see [the async adapter](#the-async-adapter-futures-io)). |
| `close(&mut self)` | Closes: closes the sink, ends every `Call` with `Closed`, cancels deferred work; then, with the lock released, waits for the sink's [`Drain`](#sink-and-framed) (for `Queued`, at most `close_timeout`). Idempotent. |
| `buffered_input(&self) -> usize` | Bytes read from the stream and held in the endpoint's decoder: part of a message, or read ahead. |
| `is_closed(&self) -> bool` | Whether the connection has ended (read without the lock). |
| `next_deadline(&self) -> Option<Instant>`, `expire(&self)` | The earliest deadline; expire what is due (with the timer off). |
| `stats(&self) -> Stats` | The connection's [counters and gauges](#stats). |

### `Endpoint::handle`

Malformed input is answered, not returned as an error. **Errors:** `Error::Closed` after `close`; `Error::Io` if the sink failed (now or since the last call), which closes the endpoint.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use std::sync::mpsc;
    use rpc_io::{Endpoint, Flow, Router};

    let router = Router::new().notification("exit", |_: &mut (), _, cx| cx.stop());
    let (tx, rx) = mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    assert_eq!(endpoint.handle(b"[]")?, Flow::Continue);
    assert!(rx.try_recv().is_ok_and(|m| m.contains("-32600")));
    assert_eq!(endpoint.handle(br#"{"jsonrpc":"2.0","method":"exit"}"#)?, Flow::Stop);
    Ok(())
}
```

### `Endpoint::run`

A message over `Limits::max_message_bytes` is skipped without being buffered and answered with `-32600` under `null`; the session goes on. When the input ends, the endpoint closes. When a handler stops it, it stays open, and the bytes already read past the stopping message stay in the endpoint: the next `run` handles them first. Interrupted reads are retried. **Errors:** `Error::Frame` for a framing the input broke beyond recovery, `Error::Io`, `Error::Closed`; the endpoint is closed after any of them.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{End, Endpoint, Framed, Framing, Router};

    let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    let mut endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), router, ());
    let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    assert_eq!(endpoint.run(input, Framing::Newline)?, End::Eof);
    let out = endpoint.with_sink(|s| s.get_ref().clone());
    assert_eq!(out, b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
    Ok(())
}
```

### `Endpoint::next_read`, `commit`, `finish`

`next_read` handles every complete message already buffered, then returns `Next::Read(buf)` (read the stream into `buf`, then `commit` the count) or `Next::Stopped`. At the end of the stream, `finish` handles what is left (a last line without its `\n`) and closes the endpoint. The read goes straight into the endpoint's decoder.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use std::io::Read;
    use rpc_io::{End, Endpoint, Framed, Framing, Next, Router};

    let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    let mut endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), router, ());
    let mut input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    let end = loop {
        let buf = match endpoint.next_read(Framing::Newline)? {
            Next::Read(buf) => buf,
            Next::Stopped => break End::Stopped,
        };
        let n = input.read(buf).map_err(rpc_io::Error::Io)?; // or `.await` on any runtime
        if n == 0 {
            break endpoint.finish()?;
        }
        endpoint.commit(n);
    };
    assert_eq!(end, End::Eof);
    Ok(())
}
```

## `Flow`, `End`, and `Next`

`Flow` (`Continue`, `Stop`) is what `handle` returns: `Stop` after a handler called `Context::stop`. `End` (`#[non_exhaustive]`: `Eof`, `Stopped`) is why `run` returned without an error: the input ended (the endpoint is closed), or a handler stopped it (the endpoint is still open, and keeps what it read ahead). `Next` (`Read(&mut [u8])`, `Stopped`) is what `next_read` needs.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{End, Endpoint, Flow, Framing, Next, Router};

    let router = Router::new().notification("pause", |_: &mut (), _, cx| cx.stop());
    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    assert_eq!(endpoint.handle(br#"{"jsonrpc":"2.0","method":"pause"}"#)?, Flow::Stop);
    let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"method\":\"pause\"}\n";
    assert_eq!(endpoint.run(input, Framing::Newline)?, End::Stopped); // still open
    assert!(matches!(endpoint.next_read(Framing::Newline)?, Next::Read(_)));
    assert_eq!(endpoint.finish()?, End::Eof); // now closed
    assert!(endpoint.is_closed());
    Ok(())
}
```

## `Router`

```rust,ignore
pub struct Router<S>
```

| Method | Description |
|---|---|
| `new()` | No handlers. |
| `request(self, method, FnMut(&mut S, Params, &mut Context) -> Result<J, ErrorObject>)` | Answers with the return value (`J: Into<Json<'static>>`). |
| `deferred(self, method, FnMut(&mut S, Params, &mut Context, Responder))` | Answers through a [`Responder`](#responder), now or later. |
| `notification(self, method, FnMut(&mut S, Params, &mut Context))` | Handles a notification. |
| `request_typed`, `notification_typed` | The same through `serde` types (`serde`; see [typed params](#typed-params-and-results-serde)). |
| `intercept(self, FnMut(&mut S, &str, Option<&Id>, &Params) -> Result<(), ErrorObject>)` | Runs before every routed call; `Err` refuses it. |
| `on_event(self, FnMut(&mut S, &Event))` | Observes [`Event`](#event)s; its panics are contained and ignored. |
| `has_request(&self, method)`, `has_notification(&self, method)` | Lookups. |

Registering a method again replaces its handler. Requests and notifications are registered separately: a request for a method with only a notification handler is `METHOD_NOT_FOUND`, and a notification with no handler is ignored (`Event::UnknownNotification`). A panicking handler may leave the state half-updated; the endpoint does not repair it.

The interceptor runs for each request and notification after the gate and cancellation have had their say, before the handler is looked up: the place for authorization, rate limits, or an audit log. `Err(error)` answers a request with `error` (so an unauthorized caller cannot probe which methods exist) and drops a notification (`Event::Refused`).

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, ErrorCode, ErrorObject, Router};

    #[derive(Default)]
    struct Server { documents: Vec<String>, authorized: bool }

    let router = Router::new()
        .request("auth", |s: &mut Server, _, _| { s.authorized = true; Ok(()) })
        .request("document/count", |s, _, _| Ok(s.documents.len()))
        .notification("document/add", |s, params, _| {
            if let Ok(p) = params.parse() {
                s.documents.push(p["text"].as_str().unwrap_or_default().to_owned());
            }
        })
        .intercept(|s, method, _, _| {
            if s.authorized || method == "auth" {
                Ok(())
            } else {
                Err(ErrorObject::new(ErrorCode(1003), "authenticate first"))
            }
        });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, Server::default());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"document/count"}"#)?;
    assert!(rx.try_recv().is_ok_and(|m| m.contains("1003")));
    endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"auth"}"#)?;
    endpoint.handle(br#"{"jsonrpc":"2.0","method":"document/add","params":{"text":"a"}}"#)?;
    assert_eq!(endpoint.state().documents, ["a"]);
    Ok(())
}
```

## `Params`

A call's `params`: validated, a slice of the message, parsed only when asked.

| Method | Description |
|---|---|
| `parse(&self) -> Result<Value<'_>, ErrorObject>` | Parses with the endpoint's JSON budgets, duplicate keys refused; `Value::Null` when absent. The error is `INVALID_PARAMS`, ready for `?`. |
| `decode<T: FromJson>(&self) -> Result<T, ErrorObject>` | Into a jsonx-lib `FromJson` type, with the endpoint's budgets and duplicate keys refused; `T` may borrow from the message. Absent params read as `null`. The error is `INVALID_PARAMS` naming the path and offset. |
| `deserialize<T>(&self) -> Result<T, ErrorObject>` | Into a `serde` type (`serde`); absent params read as `null`. |
| `raw(&self) -> Option<&Raw>` | The JSON text. |
| `is_present(&self) -> bool` | Whether params were sent (`"params": null` reads as absent). |
| `progress_token(&self) -> Option<Raw<'_>>` | MCP's `params._meta.progressToken`, a string or an integer. |
| `work_done_token(&self) -> Option<Raw<'_>>` | LSP's `params.workDoneToken`, a string or an integer. |
| `into_raw(self) -> Option<Raw>` | The text, to forward without re-serializing. |
| `into_owned(self) -> Params<'static>` | Copies, to keep beyond the call or move to a thread. |

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};

    let router = Router::new().request("echo", |_: &mut (), params, _| {
        Ok(params.into_raw().map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"echo","params":{"a": [1, 2]}}"#)?;
    assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":{"a": [1, 2]}}"#));
    Ok(())
}
```

## `Context`

What a handler can reach while it runs.

| Method | Description |
|---|---|
| `method(&self) -> &str` | The method being handled. |
| `id(&self) -> Option<&Id>` | The request's id; `None` for a notification. |
| `notify(&self, method, params)` | Sends a notification (before this request's response). |
| `request(&self, method, params) -> Result<Call, Error>` | Sends a request. Its `Call` cannot be waited on here (`CallError::WouldDeadlock`): keep it and `try_wait` later, or wait on another thread. |
| `request_with_timeout(&self, method, params, timeout)` | The same with a deadline. |
| `peer(&self) -> Peer` | A `Peer` to keep or move to a thread. |
| `notifier(&self) -> Option<Notifier>` | Notifications bound to this request (see [`Notifier`](#notifier)); `None` for a notification. For a request answered by the handler's return value, it refuses as soon as the handler returns. |
| `progress(&self, &Params) -> Option<Progress>` | MCP progress bound to this request (see [`Progress`](#progress-and-workdone)). |
| `notify_json`, `request_json` | With `ToJson` params (see [typed params](#typed-params-and-results-tojson--fromjson)). |
| `stop(&mut self)` | Stops after this message (`Flow::Stop`, `End::Stopped`); the current response is still sent. |

`ErrorObject: From<Error>` (an `INTERNAL_ERROR`), so `cx.notify(..)?` works in a request handler.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};

    let router = Router::new().request("work", |_: &mut (), _, cx| {
        cx.notify("$/progress", rpc_io::jsonx::json!({ "token": cx.method() }))?;
        Ok("done")
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#)?;
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(sent, [
        r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"work"}}"#,
        r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#,
    ]);
    Ok(())
}
```

## `Event`

`#[non_exhaustive]`. What the endpoint handled on its own, reported to `Router::on_event` (and, with the `tracing` feature, recorded as an event).

| Variant | Meaning |
|---|---|
| `Panicked { method, message }` | A handler or the interceptor panicked; a request was answered with `INTERNAL_ERROR` (the message stays here). |
| `Invalid { error, answered }` | A malformed message, or a request reusing an in-flight id; answered when request-like, dropped when a broken response. |
| `UnknownNotification { method }` | A notification with no handler. |
| `Dropped { method }` | A notification dropped before the gate opened. |
| `Refused { method, error }` | A notification the interceptor refused. |
| `InvalidParams { method, error }` | A notification whose params did not fit its typed handler (`notification_json`, or `notification_typed` with `serde`). |
| `Unmatched { id }` | A response to an id we are not waiting on. |
| `PeerError { error }` | An error response under `null`: the peer could not read something we sent. |
| `Oversized { error }` | A message over `max_message_bytes`, skipped and answered under `null`. |
| `Cancelled { id }` | The peer cancelled one of its deferred requests. |

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Event, Router};

    let router = Router::new().on_event(|log: &mut Vec<String>, event| {
        if let Event::Unmatched { id } = event {
            log.push(format!("unmatched {id}"));
        }
    });
    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, Vec::new());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":42,"result":null}"#)?;
    assert_eq!(endpoint.state(), &["unmatched 42"]);
    Ok(())
}
```

## `Stats`

`#[non_exhaustive]`, `Copy`. A snapshot from `Endpoint::stats()` or `Peer::stats()`. The endpoint keeps the counters as plain integers under the lock it takes for every message anyway.

| Field | Kind | Counts | Worth a look |
|---|---|---|---|
| `bodies_in`, `bytes_in` | counter | message bodies handled (a batch counts once) and their bytes | `bytes_in / bodies_in` near `max_message_bytes` |
| `requests_in`, `notifications_in`, `responses_in` | counter | messages from the peer, batch elements included | |
| `invalid_in` | counter | malformed messages and messages skipped as oversized | any growth: a broken or hostile peer |
| `bodies_out`, `bytes_out` | counter | message bodies sent and their bytes | |
| `requests_out`, `notifications_out`, `responses_out` | counter | messages sent, batch elements included | |
| `errors_out` | counter | error responses sent, of `responses_out` | a large share: handlers failing |
| `timed_out`, `evicted`, `cancelled_out` | counter | our requests that ended so | timeouts: a slow or stuck peer; evictions: raise `max_pending` |
| `cancelled_in` | counter | the peer's requests it cancelled | |
| `panics` | counter | handler and interceptor panics contained | any: a bug |
| `pending`, `in_flight`, `held` | gauge | our requests awaiting answers, deferred requests being answered, messages held behind the gate | near their limits |

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};

    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"missing"}"#)?;
    let stats = endpoint.stats();
    assert_eq!((stats.requests_in, stats.errors_out), (1, 1));
    Ok(())
}
```

## `Peer`

`Clone + Send + Sync`. Sends from any thread.

| Method | Description |
|---|---|
| `notify(&self, method, params) -> Result<(), Error>` | `params`: an array, an object, or `()` for none. |
| `request(&self, method, params) -> Result<Call, Error>` | Deadline: `Config::request_timeout`, if set. |
| `request_with_timeout(&self, method, params, timeout) -> Result<Call, Error>` | With its own deadline. |
| `notify_typed`, `request_typed` | With `serde` params (`serde`). |
| `is_closed(&self) -> bool` | Whether the connection has ended. |
| `stats(&self) -> Stats` | The connection's counters and gauges. |

**Errors:** `Error::InvalidParams` for params that are not an array, an object, or null; `Error::Closed`; `Error::Io` (which closes the connection); `Error::Backlog` while the gate is shut and its budget is spent.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use std::time::Duration;
    use rpc_io::{CallError, Endpoint, Router};

    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let peer = endpoint.peer();
    let call = peer.request_with_timeout("slow", (), Duration::from_millis(10))?;
    assert_eq!(call.wait(), Err(CallError::TimedOut));
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#);
    Ok(())
}
```

## `Call`

One of our requests. Ends in exactly one outcome. Dropping it before its outcome cancels the request (frees its pending slot and tells the peer), so it is `#[must_use]`; `detach()` keeps it outstanding without a waiter instead. A `Future` on any executor (`Output = Result<Raw<'static>, CallError>`), woken when the outcome arrives; polling it again after that returns `CallError::Closed`.

| Method | Description |
|---|---|
| `id(&self) -> u64` | The id on the wire. |
| `deadline(&self) -> Option<Instant>` | The deadline, if any. |
| `wait(self) -> Result<Raw<'static>, CallError>` | Blocks for the outcome, enforcing the deadline. On the endpoint's own thread returns `WouldDeadlock` unless the outcome is already there. |
| `wait_json<T: FromJson>(self) -> Result<T, CallError>` | `wait`, then decodes the result (owned `T`); one that does not fit is `CallError::InvalidResponse`. |
| `wait_typed<T>(self) -> Result<T, CallError>` | `wait`, then deserializes the result (`serde`). |
| `try_wait(self) -> Result<Result<Raw<'static>, CallError>, Call>` | Never blocks; `Err(self)` while outstanding. |
| `cancel(self) -> bool` | Cancels (sends the cancel notification); `false` if already ended. |
| `detach(self)` | Keeps the request outstanding without a waiter: answered (and discarded), timed out, or evicted later. |
| `canceller(&self) -> Canceller` | A handle that cancels from elsewhere. |

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};

    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint.peer().request("x", ())?;
    let call = match call.try_wait() { Ok(_) => unreachable!(), Err(call) => call };
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[42]}"#)?;
    assert_eq!(call.wait().map(|r| r.as_str().to_owned()), Ok("[42]".to_owned()));
    Ok(())
}
```

## `Canceller`

`Clone + Send + Sync`: `cancel(&self) -> bool` (as `Call::cancel`) and `id(&self) -> u64`.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{CallError, Endpoint, Router};

    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint.peer().request("slow", ())?;
    let canceller = call.canceller();
    std::thread::spawn(move || canceller.cancel());
    assert_eq!(call.wait(), Err(CallError::Cancelled));
    Ok(())
}
```

## `Responder`

The right to answer one deferred request, exactly once. `Send`.

| Method | Description |
|---|---|
| `id(&self) -> &Id<'static>` | The request's id. |
| `token(&self) -> CancelToken`, `is_cancelled(&self) -> bool` | The cancellation flag. |
| `peer(&self) -> Peer` | Send progress while working. |
| `respond(self, Result<J, ErrorObject>)`, `ok(self, value)`, `err(self, error)` | Answers. **Errors:** `Error::AlreadyAnswered` (cancelled first), `Error::Closed`, `Error::Io`. |
| `ok_json(self, &T)` | Answers with a `ToJson` value, encoded in a per-thread reused buffer. |
| `ok_typed(self, &T)` | Answers with a `serde` value (`serde`); one that cannot be serialized is answered `INTERNAL_ERROR` and returns `Error::Serialize`. |
| `notifier(&self) -> Notifier` | Notifications bound to this request (see [`Notifier`](#notifier)). |
| `progress(&self, &Params) -> Option<Progress>` | MCP progress bound to this request. |

Dropped without answering, it answers `INTERNAL_ERROR`.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};

    let router = Router::new().deferred("slow", |_: &mut (), _, _, responder| {
        std::thread::spawn(move || {
            if !responder.is_cancelled() {
                responder.ok("done").ok();
            }
        });
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":7,"method":"slow"}"#)?;
    let reply = rx.recv_timeout(std::time::Duration::from_secs(5)).ok();
    assert_eq!(reply.as_deref(), Some(r#"{"jsonrpc":"2.0","id":7,"result":"done"}"#));
    Ok(())
}
```

## `Notifier`

Notifications that belong to one request, sent only while it is in flight: `notify(method, params)` and `notify_json(method, &T)` refuse with `Error::AlreadyAnswered` once the request is answered or cancelled by the peer, and `Error::Closed` once the connection closed. The check runs under the lock the response is written under, so no notification can follow the response, or a cancellation the endpoint has handled, on the wire. For a stream of notifications tied to a request, such as MCP's `subscriptions/listen`. `Clone + Send + Sync`; from `Responder::notifier` or `Context::notifier`.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Config, Endpoint, Error, Router};

    let router = Router::new().deferred("subscriptions/listen", |kept: &mut Vec<rpc_io::Responder>, _, _, responder| {
        kept.push(responder);
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, Vec::new()).with_config(Config::mcp());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"subscriptions/listen"}"#)?;
    let responder = endpoint.state_mut().remove(0);
    let notifier = responder.notifier();
    notifier.notify_json("notifications/resources/updated", &["file:///a"])?;
    responder.ok(())?; // the stream ends with its response
    assert!(matches!(notifier.notify("notifications/resources/updated", ()), Err(Error::AlreadyAnswered)));
    assert_eq!(rx.try_iter().count(), 2);
    Ok(())
}
```

## `CancelToken`

`Clone + Send + Sync + Default`: `new()`, `cancel(&self)`, `is_cancelled(&self) -> bool`. Clones share the flag. The endpoint sets it when the peer cancels the request or the connection closes.

```rust
let token = rpc_io::CancelToken::new();
let seen = token.clone();
token.cancel();
assert!(seen.is_cancelled());
```

## `Progress` and `WorkDone`

`Progress` sends MCP's `notifications/progress` for one progress token, and holds MCP's two rules for it:

- `progress` must increase with every report: `report` refuses a value that does not exceed the last one sent (by this handle or a clone) with `Error::ProgressNotIncreasing { last, next }`, and sends nothing. The check and the send happen under one lock, so reports from several threads reach the wire in increasing order.
- Reports stop when the request completes (and, on stdio, once the peer cancelled it): a handle from `Context::progress(&params)` or `Responder::progress(&params)` is bound to its request and refuses with `Error::AlreadyAnswered` once the request is answered (for a request answered by the handler's return value, as soon as the handler returns) or cancelled, checked under the lock the response is written under; `Error::Closed` once the connection closed. Both return `None` when the request carries no `_meta.progressToken` (and `Context::progress` for a notification).

`new(peer, token)` and `from_params(&params, peer)` make a handle bound to no request: the first rule holds, the second is the caller's. `token()`, and `report(progress: f64, total: Option<f64>, message: Option<&str>)`. Whole numbers are written without a fraction; a number that is not finite is `Error::Serialize`.

`WorkDone` sends LSP's `$/progress` for one work-done token: `new(peer, token)`, `from_params(&params, peer)` (when the request carries `workDoneToken`), `token()`, `begin(title, cancellable, message, percentage)`, `report(message, percentage)`, `end(message)`. Percentages are capped at 100. Both are `Clone`, and send through their `Peer` from any thread.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router, WorkDone};

    let router = Router::new().request("work", |_: &mut (), params, cx| {
        if let Some(progress) = cx.progress(&params) {
            progress.report(50.0, Some(100.0), None)?;
            assert!(progress.report(40.0, None, None).is_err()); // must increase
        }
        if let Some(work) = WorkDone::from_params(&params, cx.peer()) {
            work.begin("Working", false, None, None)?;
            work.end(Some("done"))?;
        }
        Ok(())
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"work","params":{"_meta":{"progressToken":7}}}"#)?;
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(sent[0], r#"{"jsonrpc":"2.0","method":"notifications/progress","params":{"progressToken":7,"progress":50,"total":100}}"#);
    endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"work","params":{"workDoneToken":"w"}}"#)?;
    let sent: Vec<String> = rx.try_iter().collect();
    assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"w","value":{"kind":"end","message":"done"}}}"#);
    Ok(())
}
```

## `Config`

`#[non_exhaustive]`; start from `Config::new()` (or a preset) and chain `with_*`.

| Field | Default | Meaning |
|---|---|---|
| `limits` | `Limits::new()` | Budgets on incoming messages. |
| `max_pending` | 1024 | Our requests awaiting answers; one more evicts the oldest (`CallError::Evicted`). |
| `max_in_flight` | 1024 | Deferred requests being answered; more are refused with `-32000`. |
| `max_held_bytes` | 4 MiB | Messages this side sends while the gate is shut; more fail with `Error::Backlog`. |
| `request_timeout` | none | Deadline for requests that set none. |
| `cancellation` | `Some(Cancellation::lsp())` | How cancellation is spelled; `None` turns it off (the notification then reaches handlers). |
| `gate` | none | An opening request every other request waits for. |
| `timer` | `true` | Deadlines fire on their own, from a timer thread (see [Deadlines](#deadlines)). |
| `omit_null_id` | `false` | An error for a message whose id could not be read carries `"id": null` (JSON-RPC 2.0); `true` leaves `id` out (MCP). An error response from the peer with either form is read as one under `null`. |
| `overloaded_code` | `-32000` | The code of the refusal past `max_in_flight`. |

Presets:

- `Config::lsp()`: an LSP server. `Limits::lsp()`, `Cancellation::lsp()`, `Gate::lsp()`. A client leaves the gate out (`with_gate(None)`).
- `Config::mcp()`: MCP as of its current revision, 2026-07-28, for a server or a client. `Limits::mcp()` (batches off), `Cancellation::mcp()`, **no gate** (the protocol is stateless: every request carries its protocol version in `_meta`, and there is no `initialize`), `omit_null_id`, and `overloaded_code` `-32603`. This is also the preset of a dual-era server: the specification has such a server pick each request's semantics from the request itself (an `initialize` selects the legacy ones), which belongs to the protocol layer above.
- `Config::mcp_legacy()`: a server for MCP's legacy revisions only (2025-11-25 and earlier): `Config::mcp()` plus `Gate::mcp()`. Modern clients never send `initialize`, so it refuses all their requests.

Before 0.4.0, `Config::mcp()` was the legacy preset, with the `initialize` gate and its `-32002` refusals.

```rust
use std::time::Duration;
use rpc_io::{Cancellation, Config, Gate};

let mcp = Config::new()
    .with_cancellation(Some(Cancellation::mcp()))
    .with_gate(Some(Gate::new("initialize")))
    .with_request_timeout(Some(Duration::from_secs(30)));
assert_eq!(mcp.max_pending, 1024);
assert_eq!(Config::mcp().limits.max_batch, 0);
assert!(Config::mcp().gate.is_none());
assert!(Config::mcp_legacy().gate.is_some());
```

## `Cancellation`

`#[non_exhaustive]` with public fields `method`, `id_member`, `reply: Option<ErrorCode>`. Constructors: `new(method, id_member, reply)`, `lsp()` (`$/cancelRequest {id}`, answered `-32800`), `mcp()` (`notifications/cancelled {requestId}`, no answer). The notification is consumed by the endpoint; it never reaches the router or the interceptor.

```rust
use rpc_io::{Cancellation, ErrorCode};

// A service API of your own: `$/cancel {id}`, answered with error 1005.
let custom = Cancellation::new("$/cancel", "id", Some(ErrorCode(1005)));
assert_eq!(custom.method, "$/cancel");
```

## `Gate`

`#[non_exhaustive]` with public fields `method`, `code` (default `-32002`), `allow` (notifications), `allow_requests`. Constructors: `new(method)`, `lsp()` (`initialize`, `exit` allowed), `mcp()` (MCP's legacy handshake, revisions 2025-11-25 and earlier: `initialize`, the `ping` request allowed, refusals with `-32600`, since MCP used `-32002` for "resource not found" and forbids emitting it since), `allow(self, notification)`, `allow_request(self, request)`, `with_code(self, code)`.

Until the opening request succeeds: other requests are refused with `code`, except those in `allow_requests`; notifications are dropped except those in `allow`; everything this side sends but responses is held, within `Config::max_held_bytes`, and sent in order right after the opening response. If the opening request fails (an error answer, or cancelled under a cancellation with no reply), held traffic is discarded, held requests end `Cancelled`, and the gate shuts again. A held request that times out or is cancelled is never sent; an answer to one is unmatched. A repeated opening request after success, and one in a batch, are refused with `-32600`.

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Config, Endpoint, Gate, Router};

    let router = Router::new()
        .request("initialize", |_: &mut (), _, _| Ok(rpc_io::jsonx::json!({ "capabilities": {} })))
        .request("hover", |_, _, _| Ok("text"));
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ())
        .with_config(Config::new().with_gate(Some(Gate::new("initialize").allow("exit"))));
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"hover"}"#)?;
    assert!(rx.try_recv().is_ok_and(|m| m.contains("-32002")));
    endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"initialize"}"#)?;
    endpoint.handle(br#"{"jsonrpc":"2.0","id":3,"method":"hover"}"#)?;
    assert_eq!(rx.try_iter().last().as_deref(), Some(r#"{"jsonrpc":"2.0","id":3,"result":"text"}"#));
    Ok(())
}
```

## `Limits`

`#[non_exhaustive]`, `Copy`; `Limits::new()` (also `const`), the presets, and `with_*` for each field.

| Field | Default | Bounds |
|---|---:|---|
| `max_message_bytes` | 64 MiB | One body (a Content-Length body or a line). |
| `max_header_bytes` | 8 KiB | One Content-Length header section, line ends and the terminating blank line included. |
| `max_headers` | 32 | Header lines in one section. |
| `max_batch` | 1024 | Messages in one batch; `0` turns batches off (`without_batches()`). |
| `max_skip_bytes` | `u64::MAX` | The largest announced length of an oversized Content-Length body still skipped; above it the connection closes (`FrameError::TooLargeToSkip`). |
| `json` | `jsonx_lib::Limits::new()` | Depth 128, 16 MiB strings, 1 KiB numbers, 4 Mi values; its `max_input_bytes` is replaced by `max_message_bytes`. |
| `lenient_ids` | `false` | A policy, not a budget: accept `null` and non-`i64` numeric request ids (see [Ids](#ids)); off in every preset. |

| Preset | Messages | Headers | Batch | Skip | JSON depth / strings / numbers / values |
|---|---:|---:|---:|---:|---|
| `new()`, `lsp()` | 64 MiB | 8 KiB, 32 | 1024 | any | 128 / 16 MiB / 1 KiB / 4 Mi |
| `mcp()` | 64 MiB | 8 KiB, 32 | off | any | 128 / 16 MiB / 1 KiB / 4 Mi |
| `network()` | 4 MiB | 4 KiB, 16 | 128 | 64 MiB | 64 / 1 MiB / 128 B / 256 Ki |
| `strict()` | 1 MiB | 2 KiB, 8 | 16 | 4 MiB | 32 / 256 KiB / 64 B / 64 Ki |

A handler that calls `Params::parse` builds a value tree within these budgets, so one 64 MiB array of small numbers can cost several times its size: use `network()` or `strict()` for peers across a network. `parser(&self) -> jsonx_lib::Parser` is the parser these limits configure.

```rust
use rpc_io::Limits;

const SMALL: Limits = Limits::new().with_max_message_bytes(1 << 20).with_max_batch(16);
assert_eq!(SMALL.parser().get_limits().max_input_bytes, 1 << 20);
assert_eq!(Limits::network().max_message_bytes, 4 << 20);
```

## `Sink` and `Framed`

```rust,ignore
pub trait Sink: Send + 'static {
    fn send(&mut self, message: &str) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
    fn close(&mut self) -> io::Result<()> { self.flush() }
    fn drain(&mut self) -> Option<Drain> { None }
}
```

Each `send` is one unframed body; `flush` is called at the end of each turn and after each message from another thread; `close` once: when the endpoint closes, or after a send or flush failed. A sink is called with the endpoint's lock held and must not call back into the endpoint; a sink that blocks blocks every sender. `close` should not wait for the peer: a sink with writes still in flight returns a `Drain` from `drain` (asked for once, right after `close`, when the endpoint is closed rather than failed), and the closing thread waits on it after releasing the lock. `Drain::new(impl FnOnce() + Send + 'static)` wraps the wait; `wait(self)` runs it. `Queued` returns one that waits for its writer, for at most `close_timeout`. An error, or a panic, closes the connection. Implemented for `mpsc::Sender<String>` (one string per message), [`Framed<W: Write + Send + 'static>`](#sink-and-framed), [`Queued`](#queued-and-outbox), and [`OutboxSink`](#queued-and-outbox).

`Framed` frames into a reused buffer and writes once per turn, or whenever the flush threshold (64 KiB by default) is waiting, in the sending thread. A buffer grown past twice the threshold by one large message is released after it is written. `Framed` has `new(writer, framing)`, `with_flush_threshold(bytes)`, `flush_threshold()`, `framing()`, `get_ref()`, `get_mut()`, `into_inner()`.

```rust
fn main() -> Result<(), std::io::Error> {
    use rpc_io::{Framed, Framing, Sink};

    let mut framed = Framed::new(Vec::new(), Framing::ContentLength);
    framed.send("{}")?;
    framed.flush()?;
    assert_eq!(framed.get_ref().as_slice(), b"Content-Length: 2\r\n\r\n{}");
    Ok(())
}
```

## `Queued` and `Outbox`

Outgoing queues whose senders do not wait on the peer: messages are framed into a bounded queue under the endpoint's lock, and the slow part, the write to the peer, happens elsewhere, without the lock.

- **`Queued`** (a `Sink`): `new(writer, framing)` and `with_limits(writer, framing, limits)` start a writer thread over any `Write + Send + 'static`, which writes each batch of queued messages in chunks of at most 64 KiB, then one `flush`. `framing()`, `limits()`, `pending_bytes()`, `pending_messages()`. Closing the endpoint closes the queue without waiting, under the lock; its `Drain` then waits, without the lock, for what is queued to be written: until the writer is done, has made no progress for `write_timeout`, or `close_timeout` has passed. Dropping a `Queued` that was never closed does the same. When the writer finishes, the writer is dropped; a writer stuck in a write when the wait gives up ends when that write returns. **Errors:** the I/O error from starting the thread.
- **`outbox(framing, limits) -> (OutboxSink, Outbox)`**: the same queue, drained by an async task. `OutboxSink` goes into the endpoint; `Outbox::poll_chunk(cx)` yields the next framed bytes (an empty chunk at the end), `consume(n)` records how many were written, `pending_bytes()`. With `futures-io`, `Outbox` is an `AsyncRead` and `AsyncBufRead`, and `write_to(writer)` drains it into any `AsyncWrite`. Dropping the `Outbox` fails the sink, which closes the endpoint.
- **`QueueLimits`** (`#[non_exhaustive]`, `Copy`): `max_bytes` (64 MiB), `max_messages` (65,536), `write_timeout` (60 s; `None` waits forever), `close_timeout` (10 s; `Duration::MAX` waits as long as the writes make progress), with `new()` and `with_*`. A message that would take the queue past a limit fails it (one message is always taken when it is empty). `write_timeout` is a no-progress timeout: every byte the peer takes restarts it, so a slow but steady reader is never cut off; it is checked at each send, flush, and close, which then fail. A writer blocked in a write cannot check it itself: give the writer its own write timeout (as `Server` does with its sockets) to have the write fail. A failed queue refuses everything with the same I/O error, which closes the endpoint.

```rust
fn main() -> Result<(), std::io::Error> {
    use std::task::{Context, Poll, Waker};
    use rpc_io::{Endpoint, Framing, QueueLimits, Queued, Router};

    // A thread-written sink for a byte stream:
    let endpoint = Endpoint::new(Queued::new(std::io::sink(), Framing::ContentLength)?, Router::<()>::new(), ());
    endpoint.peer().notify("ready", ()).ok();

    // An async-drained one:
    let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new().with_max_bytes(1 << 20));
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    endpoint.peer().notify("ready", ()).ok();
    let Poll::Ready(chunk) = outbox.poll_chunk(&mut Context::from_waker(Waker::noop())) else { panic!() };
    assert_eq!(chunk, b"{\"jsonrpc\":\"2.0\",\"method\":\"ready\"}\n");
    Ok(())
}
```

## Typed params and results (`ToJson` / `FromJson`)

The primary typed API: jsonx-lib's own traits, hand-implemented (no derive, no reflection, no third-party dependency), in every build.

| Item | Description |
|---|---|
| `Router::request_json(method, FnMut(&mut S, P, &mut Context) -> Result<R, ErrorObject>)` | `P: for<'de> FromJson<'de>` (owned), `R: ToJson`. Params that do not decode are `INVALID_PARAMS` without calling the handler. The result is written as for `request_encoded`. |
| `Router::request_encoded(method, FnMut(&mut S, Params, &mut Context) -> Result<R, ErrorObject>)` | `R: ToJson`, written straight into a buffer the endpoint reuses (released after a result over 64 KiB): no `Value` is built, and once the buffer has grown nothing is allocated for the result. With `Params::decode` inside, the way to take params that borrow from the message. |
| `Router::notification_json(method, FnMut(&mut S, P, &mut Context))` | Params that do not decode are reported as `Event::InvalidParams`. |
| `Params::decode::<T>()`, `Raw::decode::<T>()`, `Call::wait_json::<T>()` | Decoding; see [`Params`](#params), [`Raw`](#raw), [`Call`](#call). Errors name what did not fit, the byte offset, and the JSON Pointer path, clipped to 256 bytes (they may quote keys the peer chose). |
| `Peer::notify_json(method, &T)`, `Peer::request_json(method, &T)`, and the same on `Context` | `T: ToJson`, encoded in a per-thread reused buffer (also released after 64 KiB); params must encode to an array, an object, or `null` (`Error::InvalidParams` otherwise). |
| `Responder::ok_json(&T)` | The same, for a deferred answer from any thread. |
| `Json::encode(&T) -> Json<'static>` | Compact JSON, allocated; for error `data` and the like. Infallible. |
| `ToJson` for `Id`, `ErrorCode`, `ErrorObject`, `Raw`, `Json`; `FromJson` for `Id<'de>`, `ErrorCode`, `Raw<'de>` | An id writes as it is sent and decodes strictly (a string, borrowed when it has no escapes, or an `i64` integer); a code is an integer; an error object writes as a response carries it; a `Raw` writes its text and decodes as a value's source text, unparsed. |

Encoding runs outside the endpoint's lock: a `ToJson` impl may itself send through a `Peer` (it gets a fresh buffer), and a panic in one is contained like a handler's (a request is answered with `INTERNAL_ERROR`; a panic while encoding params unwinds to the caller, and the buffer stays usable).

```rust
use rpc_io::jsonx::{DecodeError, FromJson, Reader, ToJson, ValueWriter, Written};
use rpc_io::{Endpoint, Router};

struct Position { line: u64 }
struct Hover { line: u64 }

impl<'de> FromJson<'de> for Position {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let mut line = None;
        let mut object = reader.read_object()?;
        while let Some(key) = object.next_key()? {
            if key == "line" {
                object.field(&mut line)?;
            }
        }
        Ok(Position { line: object.required(line, "line")? })
    }
}

impl ToJson for Hover {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.object(|o| {
            o.member("contents", "markdown");
            o.member("line", &self.line);
        })
    }
}

fn main() -> Result<(), rpc_io::Error> {
    let router = Router::new().request_json("hover", |_: &mut (), p: Position, _| Ok(Hover { line: p.line }));
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"hover","params":{"line":3}}"#)?;
    assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":{"contents":"markdown","line":3}}"#));
    endpoint.peer().notify_json("log", &["indexed", "a.rs"])?;
    assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","method":"log","params":["indexed","a.rs"]}"#));
    Ok(())
}
```

## Typed params and results (`serde`)

The alternative to the traits above. With the `serde` feature, protocol types written against `serde` (`lsp-types`, MCP schema crates, your own `#[derive(Deserialize)]`) go in and out of handlers and calls. Values are read from the text the endpoint has already validated within its JSON budgets, with `serde_json`.

| Item | Description |
|---|---|
| `Router::request_typed(method, FnMut(&mut S, P, &mut Context) -> Result<R, ErrorObject>)` | `P: DeserializeOwned`, `R: Serialize`. Params that do not fit are `INVALID_PARAMS`; a result that cannot be serialized is `INTERNAL_ERROR`. |
| `Router::notification_typed(method, FnMut(&mut S, P, &mut Context))` | Params that do not fit are reported as `Event::InvalidParams`. |
| `Params::deserialize::<T>()`, `Raw::deserialize::<T>()` | `INVALID_PARAMS` saying what did not fit. |
| `Json::serialize(&T) -> Result<Json<'static>, ErrorObject>` | Compact JSON, to send. |
| `Peer::notify_typed(method, &T)`, `Peer::request_typed(method, &T)` | `Error::Serialize` for a value that is not JSON. |
| `Call::wait_typed::<T>()` | A result that does not fit is `CallError::InvalidResponse`. |
| `Responder::ok_typed(&T)` | See [`Responder`](#responder). |
| `Serialize`/`Deserialize` for `Id<'static>` and `ErrorCode` | An integer or a string; an integer. |

```rust
#[cfg(not(feature = "serde"))] fn main() {}
#[cfg(feature = "serde")] fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{Endpoint, Router};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct HoverParams { text_document: Document, position: Position }
    #[derive(Deserialize)]
    struct Document { uri: String }
    #[derive(Deserialize, Serialize)]
    struct Position { line: u32, character: u32 }
    #[derive(Serialize)]
    struct Hover { contents: String }

    let router = Router::new().request_typed("textDocument/hover", |_: &mut (), p: HoverParams, _| {
        Ok(Hover { contents: format!("{} line {}", p.text_document.uri, p.position.line) })
    });
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, router, ());
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"textDocument/hover","params":{"textDocument":{"uri":"a.rs"},"position":{"line":3,"character":0}}}"#)?;
    assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":{"contents":"a.rs line 3"}}"#));

    let call = endpoint.peer().request_typed("cursor", &Position { line: 1, character: 2 })?;
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":{"line":5,"character":6}}"#)?;
    assert_eq!(call.wait_typed::<Position>().map(|p| p.line), Ok(5));
    Ok(())
}
```

## The async adapter (`futures-io`)

Runtime-agnostic: `futures-io`'s `AsyncRead`/`AsyncWrite` are implemented by smol and async-std, and reached from tokio through `tokio-util`'s `compat`. Without the feature, the same loop is a few lines over `Endpoint::next_read` and `Outbox::poll_chunk`.

- `Endpoint::run_async(&mut self, input: impl AsyncRead + Unpin, framing) -> Result<End, Error>`: `run` for an async stream. Handlers run inside the future and must not block; answer slow requests from a deferred handler's worker.
- `Outbox::write_to(&mut self, writer: impl AsyncWrite + Unpin) -> io::Result<()>`: writes everything the outbox yields, flushing after each batch, until the endpoint closes. A writer failure fails the queue, which closes the endpoint.
- `Call` is a `Future` with or without the feature.

```rust
#[cfg(not(feature = "futures-io"))] fn main() {}
#[cfg(feature = "futures-io")] fn main() -> Result<(), rpc_io::Error> {
    use std::{future::Future, pin::pin, sync::Arc, task::{Context, Poll, Wake, Waker}};
    use rpc_io::{End, Endpoint, Framing, QueueLimits, Router};

    // Any executor will do; this one parks the thread between polls.
    fn block_on<F: Future>(future: F) -> F::Output {
        struct Unpark(std::thread::Thread);
        impl Wake for Unpark { fn wake(self: Arc<Self>) { self.0.unpark() } }
        let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
        let mut future = pin!(future);
        loop {
            if let Poll::Ready(out) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                return out;
            }
            std::thread::park();
        }
    }

    let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    let (sink, mut outbox) = rpc_io::outbox(Framing::ContentLength, QueueLimits::new());
    let mut endpoint = Endpoint::new(sink, router, ());
    let input: &[u8] = b"Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}";
    assert_eq!(block_on(endpoint.run_async(input, Framing::ContentLength))?, End::Eof);
    let mut output = Vec::new();
    block_on(outbox.write_to(&mut output)).map_err(rpc_io::Error::Io)?;
    assert!(output.ends_with(br#"{"jsonrpc":"2.0","id":1,"result":"pong"}"#));
    Ok(())
}
```

## `Framing`

`#[non_exhaustive]`: `ContentLength` (LSP's base protocol, and DAP's: `Content-Length: n\r\n\r\n` then `n` bytes; other headers ignored, names case-insensitive, bare `\n` accepted, names must be RFC 9110 tokens; blank lines between messages skipped and not counted against `max_header_bytes`) and `Newline` (MCP stdio, JSON Lines: one message per line, `\r` before `\n` dropped, blank lines skipped; every line, blank or not, counts against `max_message_bytes`). Either way the result does not depend on how the input was split into reads. DAP's messages are not JSON-RPC: the framing cuts them, the endpoint does not serve them.

- `encode(self, body: &[u8], out: &mut Vec<u8>)` appends one framed message. For `Newline`, `\r` and `\n` in the body become spaces: JSON can only hold them as whitespace, so the message means the same and stays on one line.
- `decoder(self, limits: &Limits) -> Decoder`.

`READ_CHUNK` (8 KiB) is the read size the decoder offers by default, and its read-ahead past a message.

```rust
use rpc_io::Framing;

let mut out = Vec::new();
Framing::Newline.encode(b"{\n  \"a\": 1\n}", &mut out);
assert_eq!(out, b"{   \"a\": 1 }\n");
```

## `Decoder`

Sans-IO. Bytes go in through `space` + `commit` (read straight into the buffer) or `feed` (copy); bodies come out of `next_frame` as slices of the buffer. It takes bytes only after `next_frame` returned `Ok(None)`, which keeps it bounded: at most `max(max_message_bytes + 1, max_header_bytes) + read_chunk` bytes (`memory_bound`), a body reserved as it arrives (never from the header's word alone, and geometrically, so a large message costs linear copying), an oversized message skipped as it arrives. Shrink policy: once a buffer of at least four read chunks needs at most a quarter of its size, it is cut down, so memory follows the current message, not the largest one seen.

| Method | Description |
|---|---|
| `new(framing, &limits)`, `framing()` | Construction. |
| `with_read_chunk(self, bytes)`, `read_chunk()` | The bytes offered per read (at least 1; `READ_CHUNK` by default). |
| `space(&mut self) -> &mut [u8]`, `commit(&mut self, n)` | Read into the buffer. `space` is empty until `next_frame` returns `Ok(None)`, and after a fatal error. |
| `feed(&mut self, bytes) -> usize` | Copies what it takes now. |
| `next_frame(&mut self) -> Result<Option<&[u8]>, FrameError>` | The next body; `Ok(None)` when more bytes are needed. |
| `finish(&mut self) -> Result<Option<&[u8]>, FrameError>` | At end of input: a last unterminated line, `Ok(None)` when clean, `Truncated` inside a Content-Length message. |
| `buffered()`, `capacity()`, `memory_bound()` | Bytes held; bytes allocated now; the most it can hold. |

```rust
fn main() -> Result<(), rpc_io::FrameError> {
    use std::io::Read;
    use rpc_io::{Framing, Limits};

    let mut input: &[u8] = b"Content-Length: 2\r\n\r\n{}Content-Length: 4\r\n\r\nnull";
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    let mut bodies = Vec::new();
    loop {
        while let Some(body) = decoder.next_frame()? {
            bodies.push(body.to_vec());
        }
        let n = input.read(decoder.space()).unwrap_or(0);
        if n == 0 {
            break;
        }
        decoder.commit(n);
    }
    assert_eq!(decoder.finish()?, None);
    assert_eq!(bodies, [b"{}".to_vec(), b"null".to_vec()]);
    Ok(())
}
```

## `FrameError`

`#[non_exhaustive]`, `Copy`. `is_recoverable()` is true only for `TooLarge`; every other error is final and repeats.

| Variant | Meaning |
|---|---|
| `HeaderTooLarge { limit }` | A header section over `max_header_bytes`. |
| `TooManyHeaders { limit }` | More than `max_headers` lines. |
| `InvalidHeader` | A line that is not `name: value` with a token name (a body that ran into the headers). |
| `MissingContentLength` | No `Content-Length` in a section. |
| `DuplicateContentLength` | More than one, even if equal. |
| `InvalidContentLength` | Not a decimal integer that fits in 64 bits. |
| `TooLarge { length, limit }` | A message over `max_message_bytes`, skipped (recoverable). `length` is the announced Content-Length, and always `None` for newline framing (a line is refused as soon as it outgrows the limit, before its length is known). |
| `TooLargeToSkip { length, limit }` | An announced Content-Length over both `max_message_bytes` and `max_skip_bytes`: too large to read or to skip. |
| `Truncated` | The stream ended inside a Content-Length message. |

```rust
use rpc_io::{FrameError, Framing, Limits};

let mut d = Framing::ContentLength.decoder(&Limits::new().with_max_message_bytes(3));
d.feed(b"Content-Length: 5\r\n\r\nhelloContent-Length: 2\r\n\r\nok");
assert_eq!(d.next_frame(), Err(FrameError::TooLarge { length: Some(5), limit: 3 }));
assert_eq!(d.next_frame(), Ok(Some(&b"ok"[..])));
```

## `decode`

```rust,ignore
pub fn decode<'a>(body: &'a [u8], limits: &Limits) -> Incoming<'a>
```

Decodes one body: a single message or a batch, in one pass of `jsonx-lib`'s pull reader. The body is validated in full first, so a syntax error anywhere is one `PARSE_ERROR`, except in a batch refused early: over `max_batch` (refused as soon as it passes the limit, the rest unread) or while batches are off (refused at its `[`). Rules beyond the specification: `jsonrpc` must be exactly `"2.0"`; unknown members are ignored; a repeated envelope member makes the message invalid; request ids are strings or `i64` integers (any number or `null` with `Limits::lenient_ids`); an error response without `id` (MCP's form) is one under `null`; `"params": null` reads as absent; a message over a JSON budget is `INVALID_REQUEST`, under its id when the id and method were read first. `params`, `result`, and error `data` are slices of `body`.

```rust
use rpc_io::{decode, Incoming, Limits, Message};

let body = br#"{"jsonrpc": "2.0", "id": "a", "method": "add", "params": [2, 3]}"#;
let Incoming::Single(Ok(Message::Request(r))) = decode(body, &Limits::new()) else { panic!() };
assert_eq!(r.params.as_ref().map(|p| p.as_str()), Some("[2, 3]"));
```

## `Incoming` and `Entry`

`Entry<'a> = Result<Message<'a>, Invalid<'a>>`. `Incoming` is `Single(Entry)` or `Batch(Vec<Entry>)` (never empty).

```rust
use rpc_io::{decode, Incoming, Limits};

let Incoming::Batch(entries) = decode(br#"[{"jsonrpc":"2.0","method":"a"}, 7]"#, &Limits::new()) else { panic!() };
assert!(entries[0].is_ok());
assert!(entries[1].as_ref().is_err_and(|invalid| invalid.reply().is_some())); // answered with -32600
```

## `Message`

`Request(Request)`, `Notification(Notification)`, `Response(Response)`. Methods: `id()`, `method()`, `write(&self, out: &mut String)` (compact JSON), `into_owned()`; `Display` writes the JSON.

```rust
use rpc_io::{decode, Incoming, Limits, Message};

let body = br#"{"jsonrpc":"2.0","id":7,"method":"hover"}"#;
let Incoming::Single(Ok(message)) = decode(body, &Limits::new()) else { panic!() };
assert_eq!(message.method(), Some("hover"));
assert_eq!(message.id().and_then(|id| id.as_i64()), Some(7));
let owned: Message<'static> = message.into_owned();
assert_eq!(owned.to_string(), r#"{"jsonrpc":"2.0","id":7,"method":"hover"}"#);
```

## `Request`, `Notification`, `Response`

Structs with public fields: `Request { id: Id, method: Cow<str>, params: Option<Raw> }`, `Notification { method, params }`, `Response { id: Option<Id>, result: Result<Raw, ErrorObject> }` (`id: None` is `null`, only for errors). All three are `#[non_exhaustive]` (since 0.4.0), so a later version can keep more of the envelope without a break: read and change the fields freely, build with the constructors, and add `..` to struct patterns. Constructors: `Request::new(id, method, params)`, `Notification::new(method, params)`, `Response::new(id, result)`, `Response::ok(id, raw)`, `Response::err(id, error)`.

```rust
fn main() -> Result<(), rpc_io::jsonx::Error> {
    use rpc_io::{ErrorObject, Message, Raw, Response};

    let ok = Response::ok(4, Raw::new("true")?);
    assert_eq!(Message::Response(ok).to_string(), r#"{"jsonrpc":"2.0","id":4,"result":true}"#);
    let failed = Response::err(None, ErrorObject::parse_error("bad JSON"));
    assert_eq!(
        Message::Response(failed).to_string(),
        r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad JSON"}}"#,
    );
    Ok(())
}
```

## `Invalid`

A body that is not a valid message. `id()` (the id to answer under, or the request a broken response meant), `is_response()` (a broken response: never answered), `error()` (`PARSE_ERROR` or `INVALID_REQUEST`), `reply()` (the response to send, `None` for a broken response), `into_owned()`.

```rust
use rpc_io::{decode, ErrorCode, Incoming, Limits};

let Incoming::Single(Err(invalid)) = decode(br#"{"jsonrpc":"2.0","id":9,"method":"m","params":5}"#, &Limits::new()) else { panic!() };
assert_eq!((invalid.id().and_then(|i| i.as_i64()), invalid.error().code()), (Some(9), ErrorCode::INVALID_REQUEST));
```

## `write_batch`

`write_batch(out: &mut String, messages: &[Message])` appends a JSON array of the messages (`[]` for none, which JSON-RPC forbids sending).

```rust
use rpc_io::{write_batch, Message, Notification};

let ping = Message::Notification(Notification::new("ping", None));
let mut out = String::new();
write_batch(&mut out, &[ping.clone(), ping]);
assert_eq!(out, r#"[{"jsonrpc":"2.0","method":"ping"},{"jsonrpc":"2.0","method":"ping"}]"#);
```

## `Id`

`Number(i64)`, `String(Cow<str>)`, and, read only with lenient ids (see [Ids](#ids)), `RawNumber(Cow<str>)` (any other JSON number, as its text) and `Null`. Exhaustive: those are every id JSON-RPC 2.0 allows. `Ord + Hash`; `as_i64()`, `as_str()`, `is_null()`, `raw_number(&str)` (a `RawNumber`, or a `Number` for an `i64` integer; `None` for text that is not one JSON number), `into_owned()`; `From` `i64`, `i32`, `u32`, `&str`, `String`, `Cow<str>`; `Display` writes the JSON form. With `serde`, `Serialize` (a `RawNumber` through `serde_json::Number`: exact within `u64`, else the nearest `f64`), and `Deserialize` for `Id<'static>` (integers and strings).

```rust
use rpc_io::Id;

assert_eq!(Id::from("a\"b").to_string(), r#""a\"b""#);
assert_eq!(Id::from(7).as_i64(), Some(7));
assert_eq!(Id::raw_number("1e3").map(|id| id.to_string()).as_deref(), Some("1e3"));
```

## `Raw`

One JSON value kept as its exact validated text. `NULL`; `new(&str)` and `new_with(&str, &Parser)` validate (trimming surrounding whitespace); `from_string(String)`; `from_value(&Value)`; `as_str()`; `kind()` (from the first byte); `parse()` and `parse_with(&Parser)`; `decode::<T>()` (a jsonx-lib `FromJson`, borrowing from the text; `jsonx::DecodeError`); `deserialize::<T>()` (`serde`); `into_owned()`. `ToJson` (the text as it is: a `Raw` from a message, `new`, `from_value`, or `Json::encode` holds a jsonx-lib `RawValue`, validated once, and is copied with no second scan; one wrapping `serde` output is validated on the way out) and `FromJson` (a value's source text, borrowed, unparsed).

```rust
fn main() -> Result<(), rpc_io::jsonx::Error> {
    let raw = rpc_io::Raw::new(r#" {"line": 3} "#)?;
    assert_eq!(raw.as_str(), r#"{"line": 3}"#);
    assert_eq!(raw.parse()?["line"], 3);
    Ok(())
}
```

## `Json`

A value to send: `Value(jsonx_lib::Value)` or `Raw(Raw)`. Everything that sends takes `impl Into<Json>`: a `json!` value, anything that converts into a `Value` (numbers, strings, booleans, `()` for `null`, `Vec`s, `Option`s), or a `Raw`. `kind()`, `into_owned()`, `Display`; `Json::serialize(&T)` with `serde`.

```rust
use rpc_io::Json;

assert_eq!(Json::from(rpc_io::jsonx::json!([1, 2])).to_string(), "[1,2]");
assert_eq!(Json::from(()).to_string(), "null");
```

## `ErrorCode`

`ErrorCode(pub i64)` with the constants in [Error codes](#error-codes); `name()` for those, `is_reserved()` for `-32768..=-32000`; `Display` as `"-32601 (method not found)"`. With `serde`, `Serialize` and `Deserialize` as the integer.

```rust
use rpc_io::ErrorCode;

assert_eq!(ErrorCode::METHOD_NOT_FOUND.to_string(), "-32601 (method not found)");
assert_eq!(ErrorCode(-32601), ErrorCode::METHOD_NOT_FOUND);
assert_eq!(ErrorCode(1001).name(), None);
assert!(ErrorCode::SERVER_OVERLOADED.is_reserved() && !ErrorCode(1).is_reserved());
```

## `ErrorObject`

The `error` member: `new(code, message)`, `with_data(impl Into<Json>)`, `parse_error`, `invalid_request`, `method_not_found(method)` (clipped to 128 bytes), `invalid_params`, `internal_error`, `code()`, `message()`, `data()`. `From<jsonx_lib::Error>` gives `INVALID_PARAMS`; `From<Error>` gives `INTERNAL_ERROR` (for `Error::Io`, only "the transport failed": the operating system's text stays on this side). A peer's error message is clipped to 4 KiB.

```rust
use rpc_io::{ErrorCode, ErrorObject};

let e = ErrorObject::new(ErrorCode(1001), "stale version").with_data(rpc_io::jsonx::json!({ "expected": 4 }));
assert_eq!(e.data().map(|d| d.as_str()), Some(r#"{"expected":4}"#));
assert_eq!(e.to_string(), "stale version (code 1001)");
```

## `Pending`

Outstanding requests by id (`no_std`). `new(capacity)`, `capacity()`, `len()`, `is_empty()`, `contains(id)`, `get(id)`, `issue(value, deadline) -> (id, evicted)`, `settle(id) -> Option<T>`, `pop_expired(now)`, `next_deadline()`, `drain()`. Ids start at 1 and count up; at capacity the oldest is evicted; each id settles at most once; deadlines are `Duration`s from an epoch the caller chooses. Every operation is `O(log n)`.

```rust
use std::time::Duration;
use rpc_io::Pending;

let mut pending = Pending::new(2);
let (a, _) = pending.issue("first", None);
let (b, _) = pending.issue("second", Some(Duration::from_secs(5)));
let (_, evicted) = pending.issue("third", None);
assert_eq!(evicted, Some((a, "first")));
assert_eq!(pending.pop_expired(Duration::from_secs(5)), Some((b, "second")));
```

## `Error`

`#[non_exhaustive]`: `Io(io::Error)` (the transport failed; closed), `Frame(FrameError)` (the input lost its framing; closed), `Closed`, `InvalidParams(Kind)` (outgoing params not an array, object, or null; nothing sent), `AlreadyAnswered` (a deferred request was answered or cancelled first; nothing sent), `Backlog` (the gate is shut and `max_held_bytes` is spent; nothing sent, the connection stays open), `Serialize(String)` (a value is not JSON: a non-finite progress number, or a typed send with `serde`; nothing sent). Implements `std::error::Error`.

```rust
use rpc_io::{Endpoint, Error, Router};

let (tx, _rx) = std::sync::mpsc::channel::<String>();
let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
assert!(matches!(endpoint.peer().notify("x", 1), Err(Error::InvalidParams(_)))); // params must be structured
endpoint.close();
assert!(matches!(endpoint.peer().notify("x", ()), Err(Error::Closed)));
```

## `CallError`

`#[non_exhaustive]`, why one of our requests produced no result: `Rejected(ErrorObject)` (the peer's error), `InvalidResponse(ErrorObject)` (a broken answer, or one that does not fit `wait_typed`'s type), `TimedOut`, `Cancelled`, `Evicted` (over `max_pending`), `Closed`, `WouldDeadlock` (waited on the endpoint's own thread; the call, consumed by `wait`, is cancelled).

```rust
fn main() -> Result<(), rpc_io::Error> {
    use rpc_io::{CallError, Endpoint, Router};

    let (tx, _rx) = std::sync::mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint.peer().request("x", ())?;
    endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"error":{"code":1,"message":"no"}}"#)?;
    assert!(matches!(call.wait(), Err(CallError::Rejected(e)) if e.message() == "no"));
    Ok(())
}
```

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `std` | yes | The endpoint and everything it uses (`Endpoint`, `serve`, `Router`, handles, `Config`, `Sink`, `Framed`, `Queued`, `outbox`, `Progress`, `WorkDone`, `Notifier`, `Stats`, `Error`, `CallError`, and the typed handler methods); forwards to `jsonx-lib/std`. Without it, the crate is `no_std` + `alloc`: framing, the message model, `Pending`, `Raw::decode`, `Json::encode`, and the `ToJson`/`FromJson` impls for the crate's values. |
| `serde` | no | Typed params and results (above); adds `serde` and `serde_json`. |
| `futures-io` | no | The async adapter (above); adds `futures-io`. |
| `tracing` | no | A debug span per call (method and id) and events for what the endpoint does on its own, under the `rpc_io` target. Records methods, ids, sizes, error codes and messages, and panic messages; never `params`, results, or error `data` as such. Peer-chosen text is clipped and escaped (`Debug`), and an error message that quotes the peer's input (a `serde` error) is clipped to 256 bytes. Adds `tracing`. Off, the instrumentation compiles to nothing. |
| `net` | no | `Server`, `Stopper`, `Connection`; `std` only. |
