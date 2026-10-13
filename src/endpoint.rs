//! [`Endpoint`]: one JSON-RPC connection, both ways. It decodes what the peer
//! sends, routes calls to handlers, answers them, correlates answers to our
//! own requests, and applies the protocol's cancellation and gate. [`serve`]
//! is the one-call entry point over a byte stream.

use std::{
    any::Any,
    borrow::Cow,
    fmt,
    io::{self, Read, Write},
    panic::{self, AssertUnwindSafe},
    sync::Arc,
    thread,
    time::Instant,
};

use jsonx_lib::Parser;

#[cfg(feature = "tracing")]
use crate::trace::event;
use crate::{
    config::{Cancellation, Config, Gate},
    error::{CallError, Error},
    error_object::{ErrorCode, ErrorObject, clip},
    framing::{Decoder, FrameError, Framing},
    handles::{Peer, Responder},
    id::Id,
    limits::Limits,
    link::{GateState, Link, Shared},
    message::{
        Entry, Incoming, Invalid, Message, Notification, Request, Response, decode, read_member_id,
    },
    queue::Queued,
    raw::{Json, Raw},
    router::{Context, Event, EventFn, Params, RequestHandler, Router},
    sink::Sink,
    stats::{Stats, Tally},
    trace::call_span,
};

/// Whether to keep going after a message.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Flow, Router};
///
/// let (tx, _rx) = std::sync::mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, Router::new(), ());
/// assert_eq!(endpoint.handle(br#"{"jsonrpc":"2.0","method":"x"}"#)?, Flow::Continue);
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Flow {
    /// Read the next message.
    Continue,
    /// A handler called [`Context::stop`]: stop reading.
    Stop,
}

/// Why [`Endpoint::run`] returned without an error.
///
/// # Examples
///
/// ```
/// use rpc_io::{End, Framing, Router};
///
/// let input: &[u8] = b"";
/// assert_eq!(rpc_io::serve(input, std::io::sink(), Framing::Newline, Router::new())?, End::Eof);
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum End {
    /// The input ended cleanly between messages. The endpoint is closed.
    Eof,
    /// A handler called [`Context::stop`]. The endpoint is still open: the
    /// caller can send more, [`close`](Endpoint::close) it, drop it, or
    /// resume reading. Bytes already read past the stopping message stay in
    /// the endpoint, and the next [`run`](Endpoint::run) or
    /// [`next_read`](Endpoint::next_read) handles them first.
    Stopped,
}

/// What [`Endpoint::next_read`] needs next.
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Framing, Next, Router};
///
/// let (tx, _rx) = std::sync::mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
/// match endpoint.next_read(Framing::Newline)? {
///     Next::Read(buf) => assert!(!buf.is_empty()),
///     Next::Stopped => unreachable!("nothing has run yet"),
/// }
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Debug)]
pub enum Next<'a> {
    /// Read from the stream into this buffer, then call
    /// [`commit`](Endpoint::commit) with the count (or
    /// [`finish`](Endpoint::finish) when the stream has ended).
    Read(&'a mut [u8]),
    /// A handler called [`Context::stop`]; as [`End::Stopped`].
    Stopped,
}

/// One JSON-RPC 2.0 connection: a server, a client, or both at once.
///
/// An endpoint owns a [`Router`] (the handlers for calls the peer makes), the
/// state `S` handlers work on, and a [`Sink`] its messages go to. Feed it
/// message bodies with [`handle`](Endpoint::handle), or let
/// [`run`](Endpoint::run) read them from a byte stream; send through a
/// [`Peer`] from [`peer`](Endpoint::peer), from any thread.
///
/// What it guarantees, for any input:
///
/// - every request gets exactly one response, and no notification gets one
///   (an answer under a `null` id goes to a message whose id could not be
///   read, as JSON-RPC 2.0 prescribes, or to a request reusing the id of one
///   still being answered, which must not get a second response under it);
/// - malformed messages are answered with the spec's codes (`-32700` for
///   invalid JSON, `-32600` for an invalid envelope or an empty or oversized
///   batch, `-32601` for an unknown method), under their own id when it could
///   be read;
/// - a batch is answered with one array holding one response per request in
///   it, and nothing at all when it holds only notifications;
/// - answers to our requests are matched to them by id; an answer to an id we
///   are not waiting on (late, repeated, forged) is reported as
///   [`Event::Unmatched`] and otherwise ignored;
/// - a handler panic is contained: the request is answered with
///   `INTERNAL_ERROR` and the endpoint keeps serving (with `panic = "unwind"`,
///   the default; under `panic = "abort"` there is nothing to contain).
///
/// Handlers run on the thread that calls `handle` or `run`, one at a time, in
/// arrival order. The endpoint's lock is never held while user code runs.
/// Dropping the endpoint closes it.
///
/// # Examples
///
/// ```
/// use std::sync::mpsc;
/// use rpc_io::{Endpoint, ErrorObject, Router};
///
/// let router = Router::new().request("double", |_, params, _| {
///     let n = params.parse()?[0].as_i64().ok_or_else(|| ErrorObject::invalid_params("expected [n]"))?;
///     Ok(n * 2)
/// });
/// let (tx, rx) = mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, router, ());
/// endpoint.handle(br#"[{"jsonrpc":"2.0","id":1,"method":"double","params":[21]},
///                      {"jsonrpc":"2.0","id":2,"method":"nope"}]"#)?;
/// let reply = rx.try_recv().unwrap_or_default();
/// assert!(reply.starts_with(r#"[{"jsonrpc":"2.0","id":1,"result":42},{"jsonrpc":"2.0","id":2,"error":{"code":-32601"#));
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub struct Endpoint<S, K: Sink> {
    shared: Arc<Shared<K>>,
    link: Arc<dyn Link>,
    router: Router<S>,
    state: S,
    limits: Limits,
    parser: Parser,
    gate: Option<Gate>,
    cancellation: Option<Cancellation>,
    /// The stream decoder, kept between reads so read-ahead is never lost.
    input: Option<Decoder>,
}

impl<S, K: Sink> Endpoint<S, K> {
    /// An endpoint that sends through `sink`, answers calls with `router`,
    /// and gives handlers `state`, with the default [`Config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Framed, Framing, Router};
    ///
    /// let endpoint = Endpoint::new(Framed::new(std::io::stdout(), Framing::ContentLength), Router::new(), 0u32);
    /// assert_eq!(*endpoint.state(), 0);
    /// ```
    #[must_use]
    pub fn new(sink: K, router: Router<S>, state: S) -> Self {
        let config = Config::new();
        let shared = Shared::new(sink, &config);
        let link: Arc<dyn Link> = Arc::clone(&shared) as Arc<dyn Link>;
        Self {
            shared,
            link,
            router,
            state,
            limits: config.limits,
            parser: config.limits.parser(),
            gate: config.gate,
            cancellation: config.cancellation,
            input: None,
        }
    }

    /// Applies `config`. Call it before any message is sent or handled: the
    /// capacity of the pending map changes only while nothing is outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Config, Endpoint, Gate, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ())
    ///     .with_config(Config::new().with_gate(Some(Gate::new("initialize"))));
    /// endpoint.handle(br#"{"jsonrpc":"2.0","id":1,"method":"hover"}"#)?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.contains("-32002")));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn with_config(mut self, config: Config) -> Self {
        self.shared.lock().configure(&config);
        self.limits = config.limits;
        self.parser = config.limits.parser();
        self.gate = config.gate;
        self.cancellation = config.cancellation;
        if let Some(decoder) = self.input.take() {
            self.input = Some(decoder.carried(decoder.framing(), &self.limits));
        }
        self
    }

    /// A handle for sending to the peer, from any thread.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = Endpoint::new(tx, Router::new(), ());
    /// endpoint.peer().notify("ready", ())?;
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","method":"ready"}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn peer(&self) -> Peer {
        Peer::new(Arc::clone(&self.link))
    }

    /// The handlers' state.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), vec![1]);
    /// assert_eq!(endpoint.state(), &[1]);
    /// ```
    #[inline]
    #[must_use]
    pub const fn state(&self) -> &S {
        &self.state
    }

    /// The handlers' state, mutably.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), 1);
    /// *endpoint.state_mut() += 1;
    /// assert_eq!(*endpoint.state(), 2);
    /// ```
    #[inline]
    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    /// Runs `f` on the sink, under the endpoint's lock: to inspect what a
    /// [`Framed`](crate::Framed) over a `Vec` has written, for example. `f` must not use the
    /// endpoint or its handles.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Framed, Framing, Router};
    ///
    /// let endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), Router::new(), ());
    /// endpoint.peer().notify("x", ())?;
    /// let written = endpoint.with_sink(|sink| sink.get_ref().clone());
    /// assert_eq!(written, b"{\"jsonrpc\":\"2.0\",\"method\":\"x\"}\n");
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn with_sink<R>(&self, f: impl FnOnce(&mut K) -> R) -> R {
        f(self.shared.lock().sink_mut())
    }

    /// Whether the connection has ended.
    ///
    /// # Examples
    ///
    /// ```
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ());
    /// assert!(!endpoint.is_closed());
    /// endpoint.close();
    /// assert!(endpoint.is_closed());
    /// ```
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.shared.is_closed()
    }

    /// Closes the connection: closes the sink (which writes what it holds;
    /// see [`Sink::close`]), ends every outstanding [`Call`](crate::Call) with
    /// [`CallError::Closed`], and sets the [`CancelToken`](crate::CancelToken)
    /// of every deferred request still being answered. Every later send fails
    /// with [`Error::Closed`]. Idempotent.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Error, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ());
    /// endpoint.close();
    /// assert!(matches!(endpoint.peer().notify("x", ()), Err(Error::Closed)));
    /// ```
    pub fn close(&mut self) {
        self.shared.lock().close();
    }

    /// When the earliest of our outstanding requests is due, if any has a
    /// deadline. Deadlines fire on their own (see [`Config::timer`]); with the
    /// timer off, a caller calls [`expire`](Endpoint::expire) then.
    /// [`Call::wait`](crate::Call::wait) enforces its own deadline either way.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ());
    /// assert!(endpoint.next_deadline().is_none());
    /// let _call = endpoint.peer().request_with_timeout("x", (), Duration::from_secs(9))?;
    /// assert!(endpoint.next_deadline().is_some());
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn next_deadline(&self) -> Option<Instant> {
        self.shared.lock().next_deadline()
    }

    /// Ends every one of our requests whose deadline has passed with
    /// [`CallError::TimedOut`], telling the peer to cancel each.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    /// use rpc_io::CallError;
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let endpoint = rpc_io::Endpoint::new(tx, rpc_io::Router::new(), ());
    /// let call = endpoint.peer().request_with_timeout("x", (), Duration::ZERO)?;
    /// endpoint.expire();
    /// assert_eq!(call.try_wait().ok(), Some(Err(CallError::TimedOut)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn expire(&self) {
        self.shared.lock().expire();
    }

    /// The connection's counters and gauges; see [`Stats`].
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// endpoint.handle(b"{oops")?;
    /// let stats = endpoint.stats();
    /// assert_eq!((stats.bodies_in, stats.invalid_in, stats.errors_out), (1, 1, 1));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    #[must_use]
    pub fn stats(&self) -> Stats {
        self.shared.lock().stats()
    }

    /// Handles one message body (a message or a batch, unframed): routes
    /// calls to handlers, answers requests, settles answers to our requests.
    /// Everything sent during it is flushed before it returns.
    ///
    /// Malformed input is answered, not returned as an error: the errors are
    /// about the connection.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] if the endpoint was closed before; [`Error::Io`] if
    /// the sink failed (now or since the last call), which closes it.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Router};
    ///
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::new(), ());
    /// endpoint.handle(b"{not json")?;
    /// assert!(rx.try_recv().is_ok_and(|m| m.starts_with(r#"{"jsonrpc":"2.0","id":null,"error":{"code":-32700"#)));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn handle(&mut self, body: &[u8]) -> Result<Flow, Error> {
        let incoming = decode(body, &self.limits);
        self.begin_turn(&tally(&incoming, body.len()))?;
        let mut stop = false;
        match incoming {
            Incoming::Single(entry) => self.dispatch(entry, None, &mut stop),
            Incoming::Batch(entries) => {
                let owed = entries.iter().filter(|entry| owes_response(entry)).count();
                let batch = (owed > 0).then(|| self.shared.lock().open_batch(owed));
                for entry in entries {
                    self.dispatch(entry, batch, &mut stop);
                }
            }
        }
        self.end_turn()?;
        Ok(if stop { Flow::Stop } else { Flow::Continue })
    }

    /// Reads framed messages from `input` and handles each, until the input
    /// ends, a handler stops the endpoint, or the connection fails.
    ///
    /// A message over [`Limits::max_message_bytes`] is skipped without being
    /// buffered and answered with `INVALID_REQUEST` under a `null` id (its id
    /// was never read); the connection carries on. When the input ends, the
    /// endpoint closes. Interrupted reads are retried.
    ///
    /// The decoder lives in the endpoint: after [`End::Stopped`] the bytes
    /// already read past the stopping message are kept, and calling `run`
    /// again (with the same stream) handles them first. To read a different
    /// stream, or to hand the read-ahead to other code, use
    /// [`run_with`](Endpoint::run_with) and a decoder of your own.
    ///
    /// # Errors
    ///
    /// [`Error::Frame`] when the input breaks its framing beyond recovery,
    /// [`Error::Io`] when reading or writing fails, [`Error::Closed`] if the
    /// endpoint was already closed. The endpoint is closed after any of them.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{End, Endpoint, Framed, Framing, Router};
    ///
    /// let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    /// let router = Router::new().request("ping", |_, _, _| Ok("pong"));
    /// let mut endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), router, ());
    /// assert_eq!(endpoint.run(input, Framing::Newline)?, End::Eof);
    /// let out = endpoint.with_sink(|s| String::from_utf8_lossy(s.get_ref()).into_owned());
    /// assert_eq!(out, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn run<R: Read>(&mut self, mut input: R, framing: Framing) -> Result<End, Error> {
        let mut decoder = self.take_decoder(framing);
        let result = self.read_loop(&mut decoder, &mut input);
        self.settle_input(decoder, &result);
        result
    }

    /// As [`run`](Endpoint::run), with a decoder the caller owns: for a stream
    /// handed over from other code (with the bytes it already read in the
    /// decoder), or to keep the read-ahead after the endpoint is done with
    /// the stream. The endpoint's own decoder is not touched.
    ///
    /// # Errors
    ///
    /// As [`run`](Endpoint::run).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{End, Endpoint, Framing, Limits, Router};
    ///
    /// let router = Router::new().notification("upgrade", |_: &mut (), _, cx| cx.stop());
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, ());
    /// let mut decoder = Framing::Newline.decoder(&Limits::new());
    /// let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"method\":\"upgrade\"}\nraw bytes for the next owner";
    /// assert_eq!(endpoint.run_with(&mut decoder, input)?, End::Stopped);
    /// assert_eq!(decoder.buffered(), 28); // still there, for whoever reads next
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn run_with<R: Read>(&mut self, decoder: &mut Decoder, mut input: R) -> Result<End, Error> {
        let result = self.read_loop(decoder, &mut input);
        if !matches!(result, Ok(End::Stopped)) {
            self.close();
        }
        result
    }

    /// Handles every complete message already buffered, then says what is
    /// needed next: bytes from the stream, read into the buffer it returns,
    /// or nothing, because a handler stopped the endpoint.
    ///
    /// This, [`commit`](Endpoint::commit), and [`finish`](Endpoint::finish)
    /// are [`run`](Endpoint::run) taken apart, for streams `run` cannot block
    /// on: an async reader, a non-blocking socket, a poll loop. The read goes
    /// straight into the endpoint's decoder, with no copy.
    ///
    /// # Errors
    ///
    /// As [`run`](Endpoint::run); the endpoint is closed after any of them.
    ///
    /// # Examples
    ///
    /// The loop `run` runs, written out (the read can be any kind of read,
    /// `.await`ed or not):
    ///
    /// ```
    /// use std::io::Read;
    /// use rpc_io::{End, Endpoint, Framed, Framing, Next, Router};
    ///
    /// let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    /// let mut endpoint = Endpoint::new(Framed::new(Vec::new(), Framing::Newline), router, ());
    /// let mut input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    /// let end = loop {
    ///     let buf = match endpoint.next_read(Framing::Newline)? {
    ///         Next::Read(buf) => buf,
    ///         Next::Stopped => break End::Stopped,
    ///     };
    ///     let n = input.read(buf).map_err(rpc_io::Error::Io)?;
    ///     if n == 0 {
    ///         break endpoint.finish()?;
    ///     }
    ///     endpoint.commit(n);
    /// };
    /// assert_eq!(end, End::Eof);
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn next_read(&mut self, framing: Framing) -> Result<Next<'_>, Error> {
        if self.is_closed() {
            self.input = None;
            return Err(self.closed_error());
        }
        let mut decoder = self.take_decoder(framing);
        match self.drain(&mut decoder, false) {
            Ok(None) => Ok(Next::Read(self.input.insert(decoder).space())),
            Ok(Some(_)) => {
                self.input = Some(decoder);
                Ok(Next::Stopped)
            }
            Err(error) => {
                self.close();
                Err(error)
            }
        }
    }

    /// Records that `n` bytes were read into the buffer the last
    /// [`next_read`](Endpoint::next_read) returned. They are handled by the
    /// next `next_read` (or [`finish`](Endpoint::finish)).
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Endpoint, Framing, Next, Router};
    ///
    /// let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    /// let (tx, rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, router, ());
    /// let message = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    /// if let Next::Read(buf) = endpoint.next_read(Framing::Newline)? {
    ///     buf[..message.len()].copy_from_slice(message);
    /// }
    /// endpoint.commit(message.len());
    /// let _next = endpoint.next_read(Framing::Newline)?; // handles it
    /// assert_eq!(rx.try_recv().ok().as_deref(), Some(r#"{"jsonrpc":"2.0","id":1,"result":"pong"}"#));
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn commit(&mut self, n: usize) {
        if let Some(decoder) = &mut self.input {
            decoder.commit(n);
        }
    }

    /// Ends the input: handles what is left buffered (a last line without
    /// its `\n`), then closes the endpoint, unless a handler stops it first.
    ///
    /// # Errors
    ///
    /// As [`run`](Endpoint::run): [`Error::Frame`] with
    /// [`FrameError::Truncated`] if the stream ended inside a message.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{End, Endpoint, Router};
    ///
    /// let (tx, _rx) = std::sync::mpsc::channel::<String>();
    /// let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    /// assert_eq!(endpoint.finish()?, End::Eof);
    /// assert!(endpoint.is_closed());
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub fn finish(&mut self) -> Result<End, Error> {
        let Some(mut decoder) = self.input.take() else {
            if self.is_closed() {
                return Err(self.closed_error());
            }
            self.close();
            return Ok(End::Eof);
        };
        let result = if self.is_closed() {
            Err(self.closed_error())
        } else {
            self.drain(&mut decoder, true)
                .map(|end| end.unwrap_or(End::Eof))
        };
        self.settle_input(decoder, &result);
        result
    }

    /// The endpoint's decoder for `framing`, carrying over what a decoder of
    /// another framing held unread.
    fn take_decoder(&mut self, framing: Framing) -> Decoder {
        match self.input.take() {
            Some(decoder) if decoder.framing() == framing => decoder,
            Some(decoder) => decoder.carried(framing, &self.limits),
            None => framing.decoder(&self.limits),
        }
    }

    /// Keeps the decoder after a stop; closes the endpoint after anything
    /// else.
    fn settle_input(&mut self, decoder: Decoder, result: &Result<End, Error>) {
        if matches!(result, Ok(End::Stopped)) {
            self.input = Some(decoder);
        } else {
            self.close();
        }
    }

    fn read_loop<R: Read>(&mut self, decoder: &mut Decoder, input: &mut R) -> Result<End, Error> {
        if self.is_closed() {
            return Err(self.closed_error());
        }
        loop {
            if let Some(end) = self.drain(decoder, false)? {
                return Ok(end);
            }
            let space = decoder.space();
            if space.is_empty() {
                // Cannot happen once `next_frame` returned `Ok(None)`; ending
                // the input is the safe reading if it ever does.
                break;
            }
            let n = match input.read(space) {
                Ok(0) => break,
                Ok(n) => n,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(Error::Io(error)),
            };
            decoder.commit(n);
        }
        Ok(self.drain(decoder, true)?.unwrap_or(End::Eof))
    }

    /// Handles every complete message in `decoder` (at the end of the input
    /// when `finishing`); `Some(End::Stopped)` if a handler stopped.
    fn drain(&mut self, decoder: &mut Decoder, finishing: bool) -> Result<Option<End>, Error> {
        loop {
            let next = if finishing {
                decoder.finish()
            } else {
                decoder.next_frame()
            };
            let flow = match next {
                Ok(Some(body)) => self.handle(body)?,
                Ok(None) => return Ok(None),
                Err(error) if error.is_recoverable() => self.oversized(error)?,
                Err(error) => return Err(Error::Frame(error)),
            };
            if flow == Flow::Stop {
                return Ok(Some(End::Stopped));
            }
        }
    }

    /// Answers a message the framing skipped as too large.
    fn oversized(&mut self, error: FrameError) -> Result<Flow, Error> {
        self.begin_turn(&Tally {
            invalid: 1,
            ..Tally::default()
        })?;
        let answer = ErrorObject::invalid_request(format!("the message is too large: {error}"));
        self.answer(None, None, &Err(&answer));
        report(
            &mut self.router.on_event,
            &mut self.state,
            &Event::Oversized { error },
        );
        self.end_turn()?;
        Ok(Flow::Continue)
    }

    /// The error for an operation on a closed endpoint: the sink's failure
    /// if it has not been reported yet.
    fn closed_error(&self) -> Error {
        self.shared
            .lock()
            .take_broken()
            .map_or(Error::Closed, Error::Io)
    }

    fn begin_turn(&self, tally: &Tally) -> Result<(), Error> {
        let mut inner = self.shared.lock();
        if inner.is_closed() {
            return Err(inner.take_broken().map_or(Error::Closed, Error::Io));
        }
        inner.take_in(tally);
        inner.endpoint_thread = Some(thread::current().id());
        inner.cork();
        inner.expire();
        Ok(())
    }

    fn end_turn(&self) -> Result<(), Error> {
        let mut inner = self.shared.lock();
        inner.uncork();
        inner.endpoint_thread = None;
        inner
            .take_broken()
            .map_or(Ok(()), |error| Err(Error::Io(error)))
    }

    /// Sends a response. A failed send has already closed the connection and
    /// recorded the error, which the turn reports when it ends.
    fn answer(
        &self,
        id: Option<&Id<'_>>,
        batch: Option<u64>,
        result: &Result<&Json<'_>, &ErrorObject>,
    ) {
        let _reported_at_turn_end = self.shared.lock().respond(id, batch, *result);
    }

    fn dispatch(&mut self, entry: Entry<'_>, batch: Option<u64>, stop: &mut bool) {
        match entry {
            Ok(Message::Request(request)) => self.on_request(request, batch, stop),
            Ok(Message::Notification(notification)) => self.on_notification(notification, stop),
            Ok(Message::Response(response)) => self.on_response(response),
            Err(invalid) => self.on_invalid(&invalid, batch),
        }
    }

    fn on_request(&mut self, request: Request<'_>, batch: Option<u64>, stop: &mut bool) {
        let Request { id, method, params } = request;
        call_span!("request", method, Some(&id));
        match self.admit(&id, &method, batch) {
            Admission::Admitted => {}
            Admission::Refused(refusal) => {
                self.answer(Some(&id), batch, &Err(&refusal));
                return;
            }
            Admission::DuplicateId(refusal) => {
                // Under `null`: an error under the id would be a second
                // response with that id, which the peer could not tell apart
                // from the answer still to come.
                self.answer(None, batch, &Err(&refusal));
                report(
                    &mut self.router.on_event,
                    &mut self.state,
                    &Event::Invalid {
                        error: &refusal,
                        answered: true,
                    },
                );
                return;
            }
        }
        let Some(handler) = self.router.requests.get_mut(&*method) else {
            let error = ErrorObject::method_not_found(&method);
            let _reported = self.shared.lock().respond(Some(&id), batch, Err(&error));
            return;
        };
        let params = Params::new(params, self.parser);
        match handler {
            RequestHandler::Now(handler) => {
                let state = &mut self.state;
                let mut cx = Context::new(&self.link, stop, Some(&id), &method);
                let outcome = contain(|| handler(state, params, &mut cx));
                let error;
                let result = match &outcome {
                    Ok(Ok(value)) => Ok(value),
                    Ok(Err(refusal)) => Err(refusal),
                    Err(payload) => {
                        self.shared.lock().count_panic();
                        report(
                            &mut self.router.on_event,
                            &mut self.state,
                            &Event::Panicked {
                                method: &method,
                                message: &panic_message(payload.as_ref()),
                            },
                        );
                        error = ErrorObject::internal_error("the handler panicked");
                        Err(&error)
                    }
                };
                let _reported = self.shared.lock().respond(Some(&id), batch, result);
            }
            RequestHandler::Deferred(handler) => {
                let owned = id.clone().into_owned();
                let token = self.shared.lock().begin_deferred(owned.clone(), batch);
                let Some(token) = token else {
                    let error = ErrorObject::new(
                        ErrorCode::SERVER_OVERLOADED,
                        "too many requests are already being answered",
                    );
                    let _reported = self.shared.lock().respond(Some(&id), batch, Err(&error));
                    return;
                };
                let responder = Responder::new(owned, token, Arc::clone(&self.link));
                let state = &mut self.state;
                let mut cx = Context::new(&self.link, stop, Some(&id), &method);
                // A panic drops the responder while unwinding, which answers
                // the request with `INTERNAL_ERROR`.
                if let Err(payload) = contain(|| handler(state, params, &mut cx, responder)) {
                    self.shared.lock().count_panic();
                    report(
                        &mut self.router.on_event,
                        &mut self.state,
                        &Event::Panicked {
                            method: &method,
                            message: &panic_message(payload.as_ref()),
                        },
                    );
                }
            }
        }
    }

    /// Checks a request against the in-flight ids and the gate. Opens the
    /// gate's opening phase for the opening request.
    fn admit(&self, id: &Id<'_>, method: &str, batch: Option<u64>) -> Admission {
        let mut inner = self.shared.lock();
        if inner.in_flight(id) {
            return Admission::DuplicateId(ErrorObject::invalid_request(
                "the id is already used by a request still being answered",
            ));
        }
        let Some(gate) = self.gate.as_ref() else {
            return Admission::Admitted;
        };
        let opening = method == gate.method;
        let refusal = match &inner.gate {
            GateState::Open if opening => {
                ErrorObject::invalid_request(format!("`{}` was already received", clip(method)))
            }
            GateState::Open => return Admission::Admitted,
            GateState::Opening(_) if opening => ErrorObject::invalid_request(format!(
                "`{}` is already being answered",
                clip(method)
            )),
            GateState::Opening(_) => not_initialized(gate),
            GateState::Closed if !opening => not_initialized(gate),
            GateState::Closed if batch.is_some() => ErrorObject::invalid_request(format!(
                "`{}` cannot be sent in a batch",
                clip(method)
            )),
            GateState::Closed => {
                inner.gate = GateState::Opening(id.clone().into_owned());
                return Admission::Admitted;
            }
        };
        Admission::Refused(refusal)
    }

    fn on_notification(&mut self, notification: Notification<'_>, stop: &mut bool) {
        let Notification { method, params } = notification;
        call_span!("notification", method, None::<&Id<'_>>);
        if let Some(cancellation) = &self.cancellation {
            if *method == *cancellation.method {
                let target = params.as_ref().and_then(|p| {
                    read_member_id(p.as_str(), &cancellation.id_member, &self.parser)
                });
                if let Some(target) = target {
                    if self.shared.lock().cancel_incoming(&target) {
                        report(
                            &mut self.router.on_event,
                            &mut self.state,
                            &Event::Cancelled { id: &target },
                        );
                    }
                }
                return;
            }
        }
        if let Some(gate) = &self.gate {
            if self.shared.lock().gate != GateState::Open && !gate.allows(&method) {
                report(
                    &mut self.router.on_event,
                    &mut self.state,
                    &Event::Dropped { method: &method },
                );
                return;
            }
        }
        let Some(handler) = self.router.notifications.get_mut(&*method) else {
            report(
                &mut self.router.on_event,
                &mut self.state,
                &Event::UnknownNotification { method: &method },
            );
            return;
        };
        let params = Params::new(params, self.parser);
        let state = &mut self.state;
        let mut cx = Context::new(&self.link, stop, None, &method);
        if let Err(payload) = contain(|| handler(state, params, &mut cx)) {
            self.shared.lock().count_panic();
            report(
                &mut self.router.on_event,
                &mut self.state,
                &Event::Panicked {
                    method: &method,
                    message: &panic_message(payload.as_ref()),
                },
            );
        }
    }

    fn on_response(&mut self, response: Response<'_>) {
        let Response { id, result } = response;
        let Some(id) = id else {
            if let Err(error) = &result {
                report(
                    &mut self.router.on_event,
                    &mut self.state,
                    &Event::PeerError { error },
                );
            }
            return;
        };
        let settled = match ours(&id) {
            Some(number) => {
                let outcome = result.map(Raw::into_owned).map_err(CallError::Rejected);
                self.shared.lock().settle(number, outcome)
            }
            None => false,
        };
        if !settled {
            report(
                &mut self.router.on_event,
                &mut self.state,
                &Event::Unmatched { id: &id },
            );
        }
    }

    fn on_invalid(&mut self, invalid: &Invalid<'_>, batch: Option<u64>) {
        let answered = !invalid.is_response();
        if answered {
            self.answer(invalid.id(), batch, &Err(invalid.error()));
        } else if let Some(number) = invalid.id().and_then(ours) {
            // A broken answer still ends the request it names: waiting on would
            // only end in a timeout.
            let outcome = Err(CallError::InvalidResponse(invalid.error().clone()));
            let _settled = self.shared.lock().settle(number, outcome);
        }
        report(
            &mut self.router.on_event,
            &mut self.state,
            &Event::Invalid {
                error: invalid.error(),
                answered,
            },
        );
    }
}

impl<S, K: Sink> Drop for Endpoint<S, K> {
    fn drop(&mut self) {
        self.close();
    }
}

impl<S, K: Sink> fmt::Debug for Endpoint<S, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Endpoint")
            .field("router", &self.router)
            .field("limits", &self.limits)
            .field("gate", &self.gate)
            .field("cancellation", &self.cancellation)
            .finish_non_exhaustive()
    }
}

/// Serves `router` over a byte stream until the input ends or a handler
/// stops: the one-call server.
///
/// Reads framed messages from `input`, answers them through `output` with the
/// same framing, and returns when the input ends ([`End::Eof`]) or a handler
/// calls [`Context::stop`] ([`End::Stopped`]). Uses the default [`Config`];
/// for state, settings, or sending to the peer, build an [`Endpoint`].
///
/// Output goes through a [`Queued`](crate::Queued) sink under the default
/// [`QueueLimits`](crate::QueueLimits): a writer thread does the writes, so a
/// peer that stops reading stalls no handler or worker, and closes the
/// connection once the queue's limits are reached. Before returning, `serve`
/// waits for what is queued to be written (as long as the writes make
/// progress).
///
/// # Errors
///
/// As [`Endpoint::run`]; [`Error::Io`] if the writer thread cannot start.
///
/// # Examples
///
/// ```
/// use rpc_io::{End, Framing, Router};
///
/// let input: &[u8] = b"Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}";
/// let router = Router::new().request("ping", |_, _, _| Ok("pong"));
/// assert_eq!(rpc_io::serve(input, std::io::sink(), Framing::ContentLength, router)?, End::Eof);
/// # Ok::<(), rpc_io::Error>(())
/// ```
///
/// Over stdio, as an MCP or LSP server runs:
///
/// ```no_run
/// use rpc_io::{Framing, Router};
///
/// let router = Router::new().request("ping", |_, _, _| Ok("pong"));
/// rpc_io::serve(std::io::stdin().lock(), std::io::stdout(), Framing::Newline, router)?;
/// # Ok::<(), rpc_io::Error>(())
/// ```
pub fn serve<R, W>(input: R, output: W, framing: Framing, router: Router<()>) -> Result<End, Error>
where
    R: Read,
    W: Write + Send + 'static,
{
    let sink = Queued::new(output, framing)?;
    Endpoint::new(sink, router, ()).run(input, framing)
}

/// What [`Endpoint::admit`] decided about a request.
enum Admission {
    /// Route it to its handler.
    Admitted,
    /// Answer it with this error, under its id.
    Refused(ErrorObject),
    /// Its id is in flight already: answer with this error under `null`.
    DuplicateId(ErrorObject),
}

/// Whether an entry of a batch will be answered.
fn owes_response(entry: &Entry<'_>) -> bool {
    match entry {
        Ok(Message::Request(_)) => true,
        Ok(_) => false,
        Err(invalid) => !invalid.is_response(),
    }
}

/// The number of one of our requests, if `id` could be one (we issue
/// positive integers only).
fn ours(id: &Id<'_>) -> Option<u64> {
    id.as_i64()
        .and_then(|n| u64::try_from(n).ok())
        .filter(|&n| n > 0)
}

fn not_initialized(gate: &Gate) -> ErrorObject {
    ErrorObject::new(
        gate.code,
        format!(
            "`{}` must succeed before any other request",
            clip(&gate.method)
        ),
    )
}

/// Records an [`Event`] for `tracing`: no params, results, or error data,
/// which can hold anything the application sends.
#[cfg(feature = "tracing")]
fn trace_event(event: &Event<'_>) {
    match event {
        Event::Panicked { method, message } => {
            event!(error, method = %method, panic = %message, "a handler panicked");
        }
        Event::Invalid { error, answered } => {
            event!(debug, code = error.code().0, message = %error.message(), answered, "a malformed message");
        }
        Event::UnknownNotification { method } => {
            event!(debug, method = %method, "a notification with no handler");
        }
        Event::Dropped { method } => {
            event!(debug, method = %method, "a notification dropped before the gate opened");
        }
        Event::Unmatched { id } => {
            event!(debug, id = %id, "a response to no outstanding request");
        }
        Event::PeerError { error } => {
            event!(
                warn,
                code = error.code().0,
                "the peer could not read a message"
            );
        }
        Event::Oversized { error } => {
            event!(warn, error = %error, "an oversized message was skipped");
        }
        Event::Cancelled { id } => {
            event!(debug, id = %id, "the peer cancelled a request");
        }
    }
}

#[cfg(not(feature = "tracing"))]
fn trace_event(_event: &Event<'_>) {}

/// Runs user code, turning a panic into its payload.
fn contain<T>(f: impl FnOnce() -> T) -> Result<T, Box<dyn Any + Send>> {
    panic::catch_unwind(AssertUnwindSafe(f))
}

/// The text of a panic, clipped; a placeholder for non-text payloads.
fn panic_message(payload: &(dyn Any + Send)) -> Cow<'_, str> {
    if let Some(text) = payload.downcast_ref::<&str>() {
        clip(text)
    } else if let Some(text) = payload.downcast_ref::<String>() {
        clip(text)
    } else {
        Cow::Borrowed("(a panic payload that is not text)")
    }
}

/// What `incoming`, decoded from `bytes` bytes, holds, for the counters.
fn tally(incoming: &Incoming<'_>, bytes: usize) -> Tally {
    let mut tally = Tally {
        body: true,
        bytes,
        ..Tally::default()
    };
    let mut count = |entry: &Entry<'_>| match entry {
        Ok(Message::Request(_)) => tally.requests += 1,
        Ok(Message::Notification(_)) => tally.notifications += 1,
        Ok(Message::Response(_)) => tally.responses += 1,
        Err(_) => tally.invalid += 1,
    };
    match incoming {
        Incoming::Single(entry) => count(entry),
        Incoming::Batch(entries) => entries.iter().for_each(count),
    }
    tally
}

/// Tells the event hook, if any; a panic in it is contained and ignored.
/// With the `tracing` feature, records the event too.
fn report<S>(hook: &mut Option<EventFn<S>>, state: &mut S, event: &Event<'_>) {
    trace_event(event);
    if let Some(hook) = hook {
        // The hook only observes; its panic must not take the endpoint down.
        let _ignored = contain(|| hook(state, event));
    }
}
