# rpc-io &mdash; API Reference

> Complete reference for every public item in `rpc-io`, with examples.
> **Status: pre-1.0 (0.2.0).** The surface is refined across the 0.x series and
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
  - [Threads, turns, and panics](#threads-turns-and-panics)
- [`serve`](#serve)
- [`Endpoint`](#endpoint)
- [`Flow` and `End`](#flow-and-end)
- [`Router`](#router)
- [`Params`](#params)
- [`Context`](#context)
- [`Event`](#event)
- [`Peer`](#peer)
- [`Call`](#call)
- [`Canceller`](#canceller)
- [`Responder`](#responder)
- [`CancelToken`](#canceltoken)
- [`Config`](#config)
- [`Cancellation`](#cancellation)
- [`Gate`](#gate)
- [`Limits`](#limits)
- [`Sink` and `Framed`](#sink-and-framed)
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
| [`Endpoint`](#endpoint) | struct | One connection, both directions: dispatch, answers, correlation. |
| [`Flow`](#flow-and-end), [`End`](#flow-and-end) | enums | Whether to go on after a message; why `run` returned. |
| [`Router`](#router) | struct | Request, deferred, and notification handlers, and the event hook. |
| [`Params`](#params) | struct | A call's `params`, parsed on demand. |
| [`Context`](#context) | struct | What a handler can reach: method, id, the peer, `stop`. |
| [`Event`](#event) | enum | What the endpoint handled on its own. |
| [`Peer`](#peer) | struct | Send notifications and requests, from any thread. |
| [`Call`](#call), [`Canceller`](#canceller) | structs | One of our requests: await, poll, cancel. |
| [`Responder`](#responder), [`CancelToken`](#canceltoken) | structs | Answer a deferred request once; learn it was cancelled. |
| [`Config`](#config), [`Cancellation`](#cancellation), [`Gate`](#gate), [`Limits`](#limits) | structs | Settings and budgets. |
| [`Sink`](#sink-and-framed), [`Framed`](#sink-and-framed) | trait, struct | Where outgoing messages go. |
| [`Framing`](#framing), [`Decoder`](#decoder), [`FrameError`](#frameerror), `READ_CHUNK` | enum, struct, enum, const | Cut bodies out of a byte stream and frame them back. |
| [`decode`](#decode), [`Incoming`, `Entry`](#incoming-and-entry), [`Message`](#message), [`Request`, `Notification`, `Response`](#request-notification-response), [`Invalid`](#invalid), [`write_batch`](#write_batch) | the message model | Read and write JSON-RPC 2.0 messages. |
| [`Id`](#id), [`Raw`](#raw), [`Json`](#json), [`ErrorCode`](#errorcode), [`ErrorObject`](#errorobject) | values | Ids, JSON text, JSON to send, error codes and objects. |
| [`Pending`](#pending) | struct | Outstanding requests by id, with deadlines and a capacity. |
| [`Error`](#error), [`CallError`](#callerror) | enums | Why the connection, or one of our requests, failed. |

## Installation

```toml
[dependencies]
rpc-io = "0.2"
jsonx-lib = "0.2"

# Framing, messages, and correlation only, without std (alloc only):
# rpc-io = { version = "0.2", default-features = false }
```

## Quick start

```rust
use jsonx_lib::json;
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
# let _ = json!(null);
# Ok::<(), rpc_io::Error>(())
```

## Concepts

### The endpoint's contract

For any input, an [`Endpoint`](#endpoint):

- answers every request exactly once, and never answers a notification;
- answers a malformed message under its own id when the message has a `method` and a valid, unrepeated id, and under `null` otherwise; never answers a malformed *response* (it ends the call it names with `CallError::InvalidResponse` instead);
- matches answers to our requests by id, and reports answers to ids it is not waiting on as [`Event::Unmatched`](#event);
- contains handler panics: the request is answered with `INTERNAL_ERROR`, the session goes on;
- never panics, and holds memory within [`Limits`](#limits).

### Error codes

| Code | Constant | When the endpoint answers with it |
|---:|---|---|
| `-32700` | `ErrorCode::PARSE_ERROR` | The body is not valid JSON (including invalid UTF-8), or a batch has a syntax error anywhere. Always under `null`. |
| `-32600` | `ErrorCode::INVALID_REQUEST` | Valid JSON but not a valid message: wrong `jsonrpc`, non-string `method`, non-structured `params`, a bad id, a repeated envelope member, an empty or oversized batch, a message over a JSON budget or over `max_message_bytes`, a repeated opening request, an id already in flight (answered under `null`, so the in-flight request's answer stays the only response with that id). |
| `-32601` | `ErrorCode::METHOD_NOT_FOUND` | No request handler for the method. |
| `-32602` | `ErrorCode::INVALID_PARAMS` | Returned by handlers; `Params::parse` produces it for params that do not parse (duplicate keys, budgets). |
| `-32603` | `ErrorCode::INTERNAL_ERROR` | A handler panicked, or a `Responder` was dropped without answering. |
| `-32000` | `ErrorCode::SERVER_OVERLOADED` | More deferred requests than `Config::max_in_flight`. |
| `-32002` | `ErrorCode::SERVER_NOT_INITIALIZED` | A request before the [`Gate`](#gate) opened (the gate's code; this is the default). |
| `-32800` | `ErrorCode::REQUEST_CANCELLED` | The peer cancelled the request (LSP; the cancellation's code). |

### Ids

Request ids are strings or integers within `i64`. JSON-RPC 2.0 also allows `null` and fractions; LSP and MCP forbid them, and so does this crate: such a request is refused with `-32600` under `null`. Ids compare by value (`7` and `"7"` differ). Our own requests are numbered from 1 upward.

### Batches

A batch is answered with one array holding one response per request, in the order they were answered, and nothing at all when it holds only notifications and responses. An empty batch, a batch over `Limits::max_batch`, and a batch with a JSON syntax error anywhere are each answered with a single error object, not an array. A batch with deferred requests is sent when its last answer arrives. The [gate](#gate)'s opening request cannot be sent in a batch.

### Cancellation, both ways

[`Cancellation`](#cancellation) says how the protocol spells it. When the peer cancels a deferred request, the request's [`CancelToken`](#canceltoken) is set and the request is answered with the cancellation's code (LSP) or not at all (MCP); a later answer returns `Error::AlreadyAnswered`. When one of our requests is cancelled, times out, or is evicted, the peer is sent the cancel notification. Every race between an answer and a cancellation resolves to one outcome under the endpoint's lock.

### Threads, turns, and panics

Handlers run on the thread that calls `handle` or `run`, one at a time, in arrival order; that call is a *turn*, and what handlers send during it is flushed once at its end. `Peer`, `Call`, `Canceller`, and `Responder` are `Send` and work from any thread; what they send from another thread goes out at once. The endpoint's lock is held only to encode a message and hand it to the sink, never while user code runs, so handles never deadlock against a handler — except a `Sink`, which must not call back into the endpoint. Handler, event-hook, and sink panics are caught (`panic = "unwind"`, Rust's default, is required).

## `serve`

```rust,ignore
pub fn serve<R: Read, W: Write + Send + 'static>(input: R, output: W, framing: Framing, router: Router<()>) -> Result<End, Error>
```

The one-call server: reads framed messages from `input`, answers them through `output` with the same framing, and returns when the input ends (`End::Eof`) or a handler calls `Context::stop` (`End::Stopped`). Default [`Config`](#config). For state, settings, or sending to the peer, build an [`Endpoint`](#endpoint).

**Errors:** as [`Endpoint::run`](#endpointrun).

```rust
use rpc_io::{End, Framing, Router};

let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
let router = Router::new().request("ping", |_, _, _| Ok("pong"));
assert_eq!(rpc_io::serve(input, std::io::sink(), Framing::Newline, router)?, End::Eof);
# Ok::<(), rpc_io::Error>(())
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
| `close(&mut self)` | Closes: flushes, ends every `Call` with `Closed`, cancels deferred work. Idempotent. |
| `is_closed(&self) -> bool` | Whether the connection has ended. |
| `next_deadline(&self) -> Option<Instant>`, `expire(&self)` | Drive our requests' deadlines when not using `Call::wait`. |

### `Endpoint::handle`

Malformed input is answered, not returned as an error. **Errors:** `Error::Closed` after `close`; `Error::Io` if the sink failed (now or since the last call), which closes the endpoint.

```rust
use std::sync::mpsc;
use rpc_io::{Endpoint, Flow, Router};

let router = Router::new().notification("exit", |_: &mut (), _, cx| cx.stop());
let (tx, rx) = mpsc::channel::<String>();
let mut endpoint = Endpoint::new(tx, router, ());
assert_eq!(endpoint.handle(b"[]")?, Flow::Continue);
assert!(rx.try_recv().is_ok_and(|m| m.contains("-32600")));
assert_eq!(endpoint.handle(br#"{"jsonrpc":"2.0","method":"exit"}"#)?, Flow::Stop);
# Ok::<(), rpc_io::Error>(())
```

### `Endpoint::run`

A message over `Limits::max_message_bytes` is skipped without being buffered and answered with `-32600` under `null`; the session goes on. When the input ends, the endpoint closes. When a handler stops it, it stays open, and the bytes already read past the stopping message stay in the endpoint: the next `run` handles them first. Interrupted reads are retried. **Errors:** `Error::Frame` for a framing the input broke beyond recovery, `Error::Io`, `Error::Closed`; the endpoint is closed after any of them.

```rust
use rpc_io::{End, Endpoint, Framed, Framing, Router};

let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
let mut endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), router, ());
let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
assert_eq!(endpoint.run(input, Framing::Newline)?, End::Eof);
let out = endpoint.with_sink(|s| s.get_ref().clone());
assert_eq!(out, b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
# Ok::<(), rpc_io::Error>(())
```

## `Flow` and `End`

`Flow` (`Continue`, `Stop`) is what `handle` returns: `Stop` after a handler called `Context::stop`. `End` (`#[non_exhaustive]`: `Eof`, `Stopped`) is why `run` returned without an error: the input ended (the endpoint is closed), or a handler stopped it (the endpoint is still open).

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
| `on_event(self, FnMut(&mut S, &Event))` | Observes [`Event`](#event)s; its panics are contained and ignored. |
| `has_request(&self, method)`, `has_notification(&self, method)` | Lookups. |

Registering a method again replaces its handler. Requests and notifications are registered separately: a request for a method with only a notification handler is `METHOD_NOT_FOUND`, and a notification with no handler is ignored (`Event::UnknownNotification`). A panicking handler may leave the state half-updated; the endpoint does not repair it.

```rust
use jsonx_lib::json;
use rpc_io::{ErrorObject, Router};

#[derive(Default)]
struct Server { documents: Vec<String> }

let router = Router::new()
    .request("document/count", |s: &mut Server, _, _| Ok(s.documents.len()))
    .request("document/get", |s, params, _| {
        let index = params.parse()?["index"].as_u64().ok_or_else(|| ErrorObject::invalid_params("`index`"))?;
        s.documents.get(index as usize).cloned().ok_or_else(|| ErrorObject::new(rpc_io::ErrorCode(1002), "not found"))
    })
    .notification("document/add", |s, params, _| {
        if let Ok(p) = params.parse() {
            s.documents.push(p["text"].as_str().unwrap_or_default().to_owned());
        }
    });
assert!(router.has_request("document/get") && router.has_notification("document/add"));
# let _ = json!(null);
```

## `Params`

A call's `params`: validated, a slice of the message, parsed only when asked.

| Method | Description |
|---|---|
| `parse(&self) -> Result<Value<'_>, ErrorObject>` | Parses with the endpoint's JSON budgets, duplicate keys refused; `Value::Null` when absent. The error is `INVALID_PARAMS`, ready for `?`. |
| `raw(&self) -> Option<&Raw>` | The JSON text. |
| `is_present(&self) -> bool` | Whether params were sent (`"params": null` reads as absent). |
| `into_raw(self) -> Option<Raw>` | The text, to forward without re-serializing. |
| `into_owned(self) -> Params<'static>` | Copies, to keep beyond the call or move to a thread. |

```rust
use rpc_io::{Endpoint, Router};

let router = Router::new().request("echo", |_: &mut (), params, _| {
    Ok(params.into_raw().map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
});
let (tx, rx) = std::sync::mpsc::channel::<String>();
Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"echo","params":{"a": [1, 2]}}"#)?;
assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":{"a": [1, 2]}}"#));
# Ok::<(), rpc_io::Error>(())
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
| `stop(&mut self)` | Stops after this message (`Flow::Stop`, `End::Stopped`); the current response is still sent. |

`ErrorObject: From<Error>` (an `INTERNAL_ERROR`), so `cx.notify(..)?` works in a request handler.

```rust
use rpc_io::{Endpoint, Router};

let router = Router::new().request("work", |_: &mut (), _, cx| {
    cx.notify("$/progress", jsonx_lib::json!({ "token": cx.method() }))?;
    Ok("done")
});
let (tx, rx) = std::sync::mpsc::channel::<String>();
Endpoint::new(tx, router, ()).handle(br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#)?;
let sent: Vec<String> = rx.try_iter().collect();
assert_eq!(sent, [
    r#"{"jsonrpc":"2.0","method":"$/progress","params":{"token":"work"}}"#,
    r#"{"jsonrpc":"2.0","id":1,"result":"done"}"#,
]);
# Ok::<(), rpc_io::Error>(())
```

## `Event`

`#[non_exhaustive]`. What the endpoint handled on its own, reported to `Router::on_event`.

| Variant | Meaning |
|---|---|
| `Panicked { method, message }` | A handler panicked; a request was answered with `INTERNAL_ERROR` (the message stays here). |
| `Invalid { error, answered }` | A malformed message; answered when request-like, dropped when a broken response. |
| `UnknownNotification { method }` | A notification with no handler. |
| `Dropped { method }` | A notification dropped before the gate opened. |
| `Unmatched { id }` | A response to an id we are not waiting on. |
| `PeerError { error }` | An error response under `null`: the peer could not read something we sent. |
| `Oversized { error }` | A message over `max_message_bytes`, skipped and answered under `null`. |
| `Cancelled { id }` | The peer cancelled one of its deferred requests. |

```rust
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
# Ok::<(), rpc_io::Error>(())
```

## `Peer`

`Clone + Send + Sync`. Sends from any thread.

| Method | Description |
|---|---|
| `notify(&self, method, params) -> Result<(), Error>` | `params`: an array, an object, or `()` for none. |
| `request(&self, method, params) -> Result<Call, Error>` | Deadline: `Config::request_timeout`, if set. |
| `request_with_timeout(&self, method, params, timeout) -> Result<Call, Error>` | With its own deadline. |
| `is_closed(&self) -> bool` | Whether the connection has ended. |

**Errors:** `Error::InvalidParams` for params that are not an array, an object, or null; `Error::Closed`; `Error::Io` (which closes the connection).

```rust
use std::time::Duration;
use rpc_io::{CallError, Endpoint, Router};

let (tx, rx) = std::sync::mpsc::channel::<String>();
let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
let peer = endpoint.peer();
let call = peer.request_with_timeout("slow", (), Duration::from_millis(10))?;
assert_eq!(call.wait(), Err(CallError::TimedOut));
let sent: Vec<String> = rx.try_iter().collect();
assert_eq!(sent[1], r#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":1}}"#);
# Ok::<(), rpc_io::Error>(())
```

## `Call`

One of our requests. Ends in exactly one outcome. Dropping it does not cancel the request.

| Method | Description |
|---|---|
| `id(&self) -> u64` | The id on the wire. |
| `deadline(&self) -> Option<Instant>` | The deadline, if any. |
| `wait(self) -> Result<Raw<'static>, CallError>` | Blocks for the outcome, enforcing the deadline. On the endpoint's own thread returns `WouldDeadlock` unless the outcome is already there. |
| `try_wait(self) -> Result<Result<Raw<'static>, CallError>, Call>` | Never blocks; `Err(self)` while outstanding. |
| `cancel(self) -> bool` | Cancels (sends the cancel notification); `false` if already ended. |
| `canceller(&self) -> Canceller` | A handle that cancels from elsewhere. |

```rust
use rpc_io::{Endpoint, Router};

let (tx, _rx) = std::sync::mpsc::channel::<String>();
let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
let call = endpoint.peer().request("x", ())?;
let call = match call.try_wait() { Ok(_) => unreachable!(), Err(call) => call };
endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"result":[42]}"#)?;
assert_eq!(call.wait().map(|r| r.as_str().to_owned()), Ok("[42]".to_owned()));
# Ok::<(), rpc_io::Error>(())
```

## `Canceller`

`Clone + Send + Sync`: `cancel(&self) -> bool` (as `Call::cancel`) and `id(&self) -> u64`.

```rust
use rpc_io::{CallError, Endpoint, Router};

let (tx, _rx) = std::sync::mpsc::channel::<String>();
let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
let call = endpoint.peer().request("slow", ())?;
let canceller = call.canceller();
std::thread::spawn(move || canceller.cancel());
assert_eq!(call.wait(), Err(CallError::Cancelled));
# Ok::<(), rpc_io::Error>(())
```

## `Responder`

The right to answer one deferred request, exactly once. `Send`.

| Method | Description |
|---|---|
| `id(&self) -> &Id<'static>` | The request's id. |
| `token(&self) -> CancelToken`, `is_cancelled(&self) -> bool` | The cancellation flag. |
| `peer(&self) -> Peer` | Send progress while working. |
| `respond(self, Result<J, ErrorObject>)`, `ok(self, value)`, `err(self, error)` | Answers. **Errors:** `Error::AlreadyAnswered` (cancelled first), `Error::Closed`, `Error::Io`. |

Dropped without answering, it answers `INTERNAL_ERROR`.

```rust
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
# Ok::<(), rpc_io::Error>(())
```

## `CancelToken`

`Clone + Send + Sync + Default`: `new()`, `cancel(&self)`, `is_cancelled(&self) -> bool`. Clones share the flag. The endpoint sets it when the peer cancels the request or the connection closes.

```rust
let token = rpc_io::CancelToken::new();
let seen = token.clone();
token.cancel();
assert!(seen.is_cancelled());
```

## `Config`

`#[non_exhaustive]`; start from `Config::new()` and chain `with_*`.

| Field | Default | Meaning |
|---|---|---|
| `limits` | `Limits::new()` | Budgets on incoming messages. |
| `max_pending` | 1024 | Our requests awaiting answers; one more evicts the oldest (`CallError::Evicted`). |
| `max_in_flight` | 1024 | Deferred requests being answered; more are refused with `-32000`. |
| `request_timeout` | none | Deadline for requests that set none. |
| `cancellation` | `Some(Cancellation::lsp())` | How cancellation is spelled; `None` turns it off (the notification then reaches handlers). |
| `gate` | none | An opening request every other request waits for. |

```rust
use std::time::Duration;
use rpc_io::{Cancellation, Config, Gate};

let mcp = Config::new()
    .with_cancellation(Some(Cancellation::mcp()))
    .with_gate(Some(Gate::new("initialize")))
    .with_request_timeout(Some(Duration::from_secs(30)));
assert_eq!(mcp.max_pending, 1024);
```

## `Cancellation`

`#[non_exhaustive]` with public fields `method`, `id_member`, `reply: Option<ErrorCode>`. Constructors: `new(method, id_member, reply)`, `lsp()` (`$/cancelRequest {id}`, answered `-32800`), `mcp()` (`notifications/cancelled {requestId}`, no answer). The notification is consumed by the endpoint; it never reaches the router.

```rust
use rpc_io::{Cancellation, ErrorCode};

// LexerSketch's API: `$/cancel {id}`, answered with error 1005.
let studio = Cancellation::new("$/cancel", "id", Some(ErrorCode(1005)));
assert_eq!(studio.method, "$/cancel");
```

## `Gate`

`#[non_exhaustive]` with public fields `method`, `code` (default `-32002`), `allow`. Constructors: `new(method)`, `allow(self, notification)`, `with_code(self, code)`.

Until the opening request succeeds: other requests are refused with `code`; notifications are dropped except those in `allow`; everything this side sends but responses is held and sent in order right after the opening response. If the opening request fails (an error answer, or cancelled under a cancellation with no reply), held traffic is discarded, held requests end `Cancelled`, and the gate shuts again. A held request that times out or is cancelled is never sent; an answer to one is unmatched. A repeated opening request after success, and one in a batch, are refused with `-32600`. Held messages are not size-limited (requests are, by `max_pending`).

```rust
use rpc_io::{Config, Endpoint, Gate, Router};

let router = Router::new()
    .request("initialize", |_: &mut (), _, _| Ok(jsonx_lib::json!({ "capabilities": {} })))
    .request("hover", |_, _, _| Ok("text"));
let (tx, rx) = std::sync::mpsc::channel::<String>();
let mut endpoint = Endpoint::new(tx, router, ())
    .with_config(Config::new().with_gate(Some(Gate::new("initialize").allow("exit"))));
endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"hover"}"#)?;
assert!(rx.try_recv().is_ok_and(|m| m.contains("-32002")));
endpoint.handle(br#"{"jsonrpc":"2.0","id":2,"method":"initialize"}"#)?;
endpoint.handle(br#"{"jsonrpc":"2.0","id":3,"method":"hover"}"#)?;
assert_eq!(rx.try_iter().last().as_deref(), Some(r#"{"jsonrpc":"2.0","id":3,"result":"text"}"#));
# Ok::<(), rpc_io::Error>(())
```

## `Limits`

`#[non_exhaustive]`, `Copy`; `Limits::new()` (also `const`) and `with_*` for each field.

| Field | Default | Bounds |
|---|---:|---|
| `max_message_bytes` | 64 MiB | One body (a Content-Length body or a line). |
| `max_header_bytes` | 8 KiB | One Content-Length header section, line ends included. |
| `max_headers` | 32 | Header lines in one section. |
| `max_batch` | 1024 | Messages in one batch. |
| `json` | `jsonx_lib::Limits::new()` | Depth 128, 16 MiB strings, 1 KiB numbers, 4 Mi values; its `max_input_bytes` is replaced by `max_message_bytes`. |

`parser(&self) -> jsonx_lib::Parser` is the parser these limits configure.

```rust
use rpc_io::Limits;

const SMALL: Limits = Limits::new().with_max_message_bytes(1 << 20).with_max_batch(16);
assert_eq!(SMALL.parser().get_limits().max_input_bytes, 1 << 20);
```

## `Sink` and `Framed`

```rust,ignore
pub trait Sink: Send + 'static {
    fn send(&mut self, message: &str) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()> { Ok(()) }
}
```

Each `send` is one unframed body; `flush` is called at the end of each turn and after each message from another thread. A sink is called with the endpoint's lock held and must not call back into the endpoint. An error, or a panic, closes the connection. Implemented for `mpsc::Sender<String>` (one string per message) and [`Framed<W: Write + Send + 'static>`](#sink-and-framed), which frames into a reused buffer and writes once per turn (or whenever 64 KiB is waiting). `Framed` has `new(writer, framing)`, `framing()`, `get_ref()`, `get_mut()`, `into_inner()`.

```rust
use rpc_io::{Framed, Framing, Sink};

let mut framed = Framed::new(Vec::new(), Framing::ContentLength);
framed.send("{}")?;
framed.flush()?;
assert_eq!(framed.get_ref().as_slice(), b"Content-Length: 2\r\n\r\n{}");
# Ok::<(), std::io::Error>(())
```

## `Framing`

`#[non_exhaustive]`: `ContentLength` (LSP and DAP: `Content-Length: n\r\n\r\n` then `n` bytes; other headers ignored, names case-insensitive, bare `\n` accepted, names must be RFC 9110 tokens; blank lines between messages skipped and not counted against `max_header_bytes`) and `Newline` (MCP stdio, JSON Lines: one message per line, `\r` before `\n` dropped, blank lines skipped; every line, blank or not, counts against `max_message_bytes`). Either way the result does not depend on how the input was split into reads.

- `encode(self, body: &[u8], out: &mut Vec<u8>)` appends one framed message. For `Newline`, `\r` and `\n` in the body become spaces: JSON can only hold them as whitespace, so the message means the same and stays on one line.
- `decoder(self, limits: &Limits) -> Decoder`.

`READ_CHUNK` (8 KiB) is the read size the decoder offers and its read-ahead past a message.

```rust
use rpc_io::Framing;

let mut out = Vec::new();
Framing::Newline.encode(b"{\n  \"a\": 1\n}", &mut out);
assert_eq!(out, b"{   \"a\": 1 }\n");
```

## `Decoder`

Sans-IO. Bytes go in through `space` + `commit` (read straight into the buffer) or `feed` (copy); bodies come out of `next_frame` as slices of the buffer. It takes bytes only after `next_frame` returned `Ok(None)`, which keeps it bounded: at most `max(max_message_bytes + 1, max_header_bytes) + READ_CHUNK` bytes (`memory_bound`), a body reserved as it arrives (never from the header's word alone), an oversized message skipped as it arrives.

| Method | Description |
|---|---|
| `new(framing, &limits)`, `framing()` | Construction. |
| `space(&mut self) -> &mut [u8]`, `commit(&mut self, n)` | Read into the buffer. `space` is empty until `next_frame` returns `Ok(None)`, and after a fatal error. |
| `feed(&mut self, bytes) -> usize` | Copies what it takes now. |
| `next_frame(&mut self) -> Result<Option<&[u8]>, FrameError>` | The next body; `Ok(None)` when more bytes are needed. |
| `finish(&mut self) -> Result<Option<&[u8]>, FrameError>` | At end of input: a last unterminated line, `Ok(None)` when clean, `Truncated` inside a Content-Length message. |
| `buffered()`, `memory_bound()` | Bytes held; the most it can hold. |

```rust
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
# Ok::<(), rpc_io::FrameError>(())
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

Decodes one body: a single message or a batch, in one pass of `jsonx-lib`'s pull reader. The body is validated in full first, so a syntax error anywhere is one `PARSE_ERROR`. Rules beyond the specification: `jsonrpc` must be exactly `"2.0"`; unknown members are ignored; a repeated envelope member makes the message invalid; request ids are strings or `i64` integers; `"params": null` reads as absent; a message over a JSON budget is `INVALID_REQUEST`, under its id when the id and method were read first. `params`, `result`, and error `data` are slices of `body`.

```rust
use rpc_io::{decode, Incoming, Limits, Message};

let body = br#"{"jsonrpc": "2.0", "id": "a", "method": "add", "params": [2, 3]}"#;
let Incoming::Single(Ok(Message::Request(r))) = decode(body, &Limits::new()) else { panic!() };
assert_eq!(r.params.as_ref().map(|p| p.as_str()), Some("[2, 3]"));
```

## `Incoming` and `Entry`

`Entry<'a> = Result<Message<'a>, Invalid<'a>>`. `Incoming` is `Single(Entry)` or `Batch(Vec<Entry>)` (never empty).

## `Message`

`Request(Request)`, `Notification(Notification)`, `Response(Response)`. Methods: `id()`, `method()`, `write(&self, out: &mut String)` (compact JSON), `into_owned()`; `Display` writes the JSON.

## `Request`, `Notification`, `Response`

Plain structs with public fields: `Request { id: Id, method: Cow<str>, params: Option<Raw> }`, `Notification { method, params }`, `Response { id: Option<Id>, result: Result<Raw, ErrorObject> }` (`id: None` is `null`, only for errors).

```rust
use rpc_io::{ErrorObject, Message, Raw, Response};

let ok = Response { id: Some(4.into()), result: Ok(Raw::new("true")?) };
assert_eq!(Message::Response(ok).to_string(), r#"{"jsonrpc":"2.0","id":4,"result":true}"#);
let failed = Response { id: None, result: Err(ErrorObject::parse_error("bad JSON")) };
assert_eq!(
    Message::Response(failed).to_string(),
    r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"bad JSON"}}"#,
);
# Ok::<(), jsonx_lib::Error>(())
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

## `Id`

`Number(i64)` or `String(Cow<str>)`; `Ord + Hash`; `as_i64()`, `as_str()`, `into_owned()`; `From` `i64`, `i32`, `u32`, `&str`, `String`, `Cow<str>`; `Display` writes the JSON form.

```rust
use rpc_io::Id;

assert_eq!(Id::from("a\"b").to_string(), r#""a\"b""#);
assert_eq!(Id::from(7).as_i64(), Some(7));
```

## `Raw`

One JSON value kept as its exact validated text. `NULL`; `new(&str)` and `new_with(&str, &Parser)` validate (trimming surrounding whitespace); `from_string(String)`; `from_value(&Value)`; `as_str()`; `kind()` (from the first byte); `parse()` and `parse_with(&Parser)`; `into_owned()`.

```rust
let raw = rpc_io::Raw::new(r#" {"line": 3} "#)?;
assert_eq!(raw.as_str(), r#"{"line": 3}"#);
assert_eq!(raw.parse()?["line"], 3);
# Ok::<(), jsonx_lib::Error>(())
```

## `Json`

A value to send: `Value(jsonx_lib::Value)` or `Raw(Raw)`. Everything that sends takes `impl Into<Json>`: a `json!` value, anything that converts into a `Value` (numbers, strings, booleans, `()` for `null`, `Vec`s, `Option`s), or a `Raw`. `kind()`, `into_owned()`, `Display`.

```rust
use rpc_io::Json;

assert_eq!(Json::from(jsonx_lib::json!([1, 2])).to_string(), "[1,2]");
assert_eq!(Json::from(()).to_string(), "null");
```

## `ErrorCode`

`ErrorCode(pub i64)` with the constants in [Error codes](#error-codes); `name()` for those, `is_reserved()` for `-32768..=-32000`; `Display` as `"-32601 (method not found)"`.

## `ErrorObject`

The `error` member: `new(code, message)`, `with_data(impl Into<Json>)`, `parse_error`, `invalid_request`, `method_not_found(method)` (clipped to 128 bytes), `invalid_params`, `internal_error`, `code()`, `message()`, `data()`. `From<jsonx_lib::Error>` gives `INVALID_PARAMS`; `From<Error>` gives `INTERNAL_ERROR`. A peer's error message is clipped to 4 KiB.

```rust
use rpc_io::{ErrorCode, ErrorObject};

let e = ErrorObject::new(ErrorCode(1001), "stale version").with_data(jsonx_lib::json!({ "expected": 4 }));
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

`#[non_exhaustive]`: `Io(io::Error)` (the transport failed; closed), `Frame(FrameError)` (the input lost its framing; closed), `Closed`, `InvalidParams(Kind)` (outgoing params not an array, object, or null; nothing sent), `AlreadyAnswered` (a deferred request was answered or cancelled first; nothing sent). Implements `std::error::Error`.

## `CallError`

`#[non_exhaustive]`, why one of our requests produced no result: `Rejected(ErrorObject)` (the peer's error), `InvalidResponse(ErrorObject)` (a broken answer), `TimedOut`, `Cancelled`, `Evicted` (over `max_pending`), `Closed`, `WouldDeadlock` (waited on the endpoint's own thread).

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `std` | yes | The endpoint and everything it uses (`Endpoint`, `serve`, `Router`, handles, `Config`, `Sink`, `Framed`, `Error`, `CallError`); forwards to `jsonx-lib/std`. Without it, the crate is `no_std` + `alloc`: framing, the message model, `Pending`. |
