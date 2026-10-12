//! Message framing: how message bodies are cut out of a byte stream and put
//! back into one.
//!
//! The [`Decoder`] is sans-IO: it owns a buffer, says where the next bytes
//! should go ([`Decoder::space`]), and returns complete message bodies
//! ([`Decoder::next_frame`]). Whoever owns the stream does the reading, so the
//! same decoder serves a blocking `Read`, a non-blocking socket, or bytes that
//! arrive any other way.

use alloc::vec::Vec;
use core::{fmt, ops::Range};

use crate::limits::Limits;

/// Bytes offered for each read, and the read-ahead past a message's end.
pub const READ_CHUNK: usize = 8 * 1024;

/// How messages are delimited on a byte stream.
///
/// `#[non_exhaustive]`: HTTP bodies and WebSocket messages are planned (see
/// the roadmap).
///
/// # Examples
///
/// ```
/// use rpc_io::Framing;
///
/// let mut out = Vec::new();
/// Framing::ContentLength.encode(br#"{"jsonrpc":"2.0","method":"x"}"#, &mut out);
/// assert!(out.starts_with(b"Content-Length: 30\r\n\r\n{"));
///
/// out.clear();
/// Framing::Newline.encode(b"{\n}", &mut out);
/// assert_eq!(out, b"{ }\n");
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Framing {
    /// `Content-Length: <n>\r\n\r\n` and then `n` bytes of body: the base
    /// protocol of LSP and DAP. Other headers (`Content-Type`) are accepted and
    /// ignored; header names match case-insensitively; a bare `\n` line end is
    /// accepted for `\r\n`. Blank lines before a header section (stray line
    /// ends between messages) are skipped and do not count against
    /// [`Limits::max_header_bytes`]: each is consumed as soon as its line end
    /// arrives, so they cost no memory, and a header section is measured the
    /// same however the input was split.
    ContentLength,
    /// One message per line, ended by `\n` (an `\r` before it is dropped):
    /// MCP's stdio transport, and JSON Lines. Lines holding only whitespace are
    /// skipped, but every line counts against
    /// [`Limits::max_message_bytes`], blank or not: a line over it is
    /// [`FrameError::TooLarge`] (with `length: None`) however the input was
    /// split into reads. Blank lines are consumed as soon as their `\n`
    /// arrives, so a run of them costs no memory.
    Newline,
}

impl Framing {
    /// Appends `body` to `out` as one framed message.
    ///
    /// For [`Newline`](Framing::Newline), every `\r` and `\n` in `body` is
    /// written as a space. In valid JSON line breaks can only be whitespace
    /// (strings cannot contain them raw), so the message means the same and
    /// stays on one line; text that is not JSON still cannot split the frame.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::Framing;
    ///
    /// let mut out = Vec::new();
    /// Framing::ContentLength.encode(b"{}", &mut out);
    /// assert_eq!(out, b"Content-Length: 2\r\n\r\n{}");
    /// ```
    pub fn encode(self, body: &[u8], out: &mut Vec<u8>) {
        match self {
            Framing::ContentLength => {
                out.reserve(body.len() + 40);
                out.extend_from_slice(b"Content-Length: ");
                let (digits, at) = decimal(body.len() as u64);
                out.extend_from_slice(&digits[at..]);
                out.extend_from_slice(b"\r\n\r\n");
                out.extend_from_slice(body);
            }
            Framing::Newline => {
                out.reserve(body.len() + 1);
                let mut rest = body;
                while let Some(at) = find_line_break(rest) {
                    out.extend_from_slice(&rest[..at]);
                    out.push(b' ');
                    rest = &rest[at + 1..];
                }
                out.extend_from_slice(rest);
                out.push(b'\n');
            }
        }
    }

    /// A [`Decoder`] for this framing under `limits`.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// let mut decoder = Framing::Newline.decoder(&Limits::new());
    /// let taken = decoder.feed(b"{\"a\":1}\n{\"b\"");
    /// assert_eq!(taken, 12);
    /// assert_eq!(decoder.next_frame(), Ok(Some(&b"{\"a\":1}"[..])));
    /// assert_eq!(decoder.next_frame(), Ok(None)); // the second line is not complete yet
    /// ```
    #[must_use]
    pub fn decoder(self, limits: &Limits) -> Decoder {
        Decoder::new(self, limits)
    }
}

/// Why a byte stream could not be cut into messages.
///
/// Only [`TooLarge`](FrameError::TooLarge) is recoverable: the decoder has
/// skipped (or is skipping) the oversized message without buffering it, and
/// the next message can be read. Every other error leaves the stream with no
/// reliable message boundary, so the decoder stops and returns the same error
/// from then on.
///
/// # Examples
///
/// ```
/// use rpc_io::{FrameError, Framing, Limits};
///
/// let mut decoder = Framing::ContentLength.decoder(&Limits::new());
/// decoder.feed(b"Content-Length: 1x\r\n\r\n");
/// assert_eq!(decoder.next_frame(), Err(FrameError::InvalidContentLength));
/// assert!(!FrameError::InvalidContentLength.is_recoverable());
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FrameError {
    /// A header section exceeded [`Limits::max_header_bytes`].
    HeaderTooLarge {
        /// The limit, in bytes.
        limit: usize,
    },
    /// A header section had more than [`Limits::max_headers`] lines.
    TooManyHeaders {
        /// The limit, in lines.
        limit: usize,
    },
    /// A header line has no `:`, or its name is not an RFC 9110 token
    /// (empty, or holding a space, a brace, or another separator).
    InvalidHeader,
    /// A header section ended without a `Content-Length` header.
    MissingContentLength,
    /// A header section has more than one `Content-Length` header.
    DuplicateContentLength,
    /// A `Content-Length` value is not a decimal integer that fits in 64 bits.
    InvalidContentLength,
    /// A message exceeded [`Limits::max_message_bytes`]. Recoverable: the
    /// message is skipped without being buffered.
    TooLarge {
        /// The length the header announced; always `None` for a
        /// newline-delimited line, which is refused as soon as it outgrows the
        /// limit, before its length is known.
        length: Option<u64>,
        /// The limit, in bytes.
        limit: usize,
    },
    /// The stream ended inside a message.
    Truncated,
}

impl FrameError {
    /// Whether the decoder can go on to the next message.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::FrameError;
    ///
    /// assert!(FrameError::TooLarge { length: Some(9), limit: 8 }.is_recoverable());
    /// assert!(!FrameError::Truncated.is_recoverable());
    /// ```
    #[must_use]
    pub const fn is_recoverable(&self) -> bool {
        matches!(self, FrameError::TooLarge { .. })
    }
}

impl fmt::Display for FrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FrameError::HeaderTooLarge { limit } => {
                write!(f, "a header section is longer than {limit} bytes")
            }
            FrameError::TooManyHeaders { limit } => {
                write!(f, "a header section has more than {limit} lines")
            }
            FrameError::InvalidHeader => f.write_str("a header line is not `name: value`"),
            FrameError::MissingContentLength => {
                f.write_str("a header section has no `Content-Length`")
            }
            FrameError::DuplicateContentLength => {
                f.write_str("a header section has more than one `Content-Length`")
            }
            FrameError::InvalidContentLength => {
                f.write_str("a `Content-Length` value is not a non-negative decimal integer")
            }
            FrameError::TooLarge {
                length: Some(length),
                limit,
            } => write!(
                f,
                "a message of {length} bytes exceeds the limit of {limit}"
            ),
            FrameError::TooLarge {
                length: None,
                limit,
            } => {
                write!(f, "a message exceeds the limit of {limit} bytes")
            }
            FrameError::Truncated => f.write_str("the stream ended inside a message"),
        }
    }
}

impl core::error::Error for FrameError {}

/// Where the decoder is in the stream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    /// Reading a Content-Length header section: lines so far, bytes so far,
    /// and the length once seen.
    Headers {
        lines: usize,
        bytes: usize,
        length: Option<u64>,
    },
    /// Waiting for a body of `length` bytes, starting at `start`.
    Body { length: usize },
    /// Dropping the rest of an oversized Content-Length body.
    Skip { remaining: u64 },
    /// Reading a newline-delimited line.
    Line,
    /// Dropping the rest of an oversized line, up to its `\n`.
    Discard,
}

/// Cuts message bodies out of a byte stream (sans-IO).
///
/// Bytes go in through [`space`](Decoder::space) and
/// [`commit`](Decoder::commit) (read straight into the decoder's buffer, no
/// extra copy) or [`feed`](Decoder::feed) (copied from a slice); bodies come
/// out of [`next_frame`](Decoder::next_frame) as slices of the buffer. The
/// two alternate: the decoder takes more bytes only once `next_frame` has
/// returned `Ok(None)`, which is what keeps its memory bounded no matter how
/// the peer sends:
///
/// - the buffer never exceeds one message plus [`READ_CHUNK`] bytes of
///   read-ahead (`max(max_message_bytes + 1, max_header_bytes) + READ_CHUNK`);
/// - a Content-Length body is reserved as its bytes arrive, never all at once
///   from the header's word alone;
/// - an oversized message is skipped as it arrives ([`FrameError::TooLarge`])
///   rather than buffered;
/// - lines are scanned once: the search for a line end resumes where the last
///   one stopped, so a message delivered a byte at a time costs linear work.
///
/// # Examples
///
/// Reading from any `std::io::Read`:
///
/// ```
/// use std::io::Read;
/// use rpc_io::{Framing, Limits};
///
/// let mut input: &[u8] = b"Content-Length: 2\r\n\r\n{}Content-Length: 4\r\n\r\nnull";
/// let mut decoder = Framing::ContentLength.decoder(&Limits::new());
/// let mut bodies = Vec::new();
/// loop {
///     while let Some(body) = decoder.next_frame()? {
///         bodies.push(body.to_vec());
///     }
///     let n = input.read(decoder.space()).unwrap_or(0);
///     if n == 0 {
///         break;
///     }
///     decoder.commit(n);
/// }
/// assert_eq!(decoder.finish()?, None); // nothing left over
/// assert_eq!(bodies, [b"{}".to_vec(), b"null".to_vec()]);
/// # Ok::<(), rpc_io::FrameError>(())
/// ```
#[derive(Clone, Debug)]
pub struct Decoder {
    framing: Framing,
    max_message: usize,
    max_header_bytes: usize,
    max_headers: usize,
    /// Initialized storage; bytes `start..end` are buffered and unconsumed.
    buf: Vec<u8>,
    start: usize,
    end: usize,
    /// Where the search for the next line end resumes (`start..=end`).
    scan: usize,
    state: State,
    /// Whether `next_frame` has found nothing more since the last commit;
    /// only then is space offered.
    drained: bool,
    /// The fatal error the decoder stopped on.
    failed: Option<FrameError>,
}

impl Decoder {
    /// A decoder for `framing` under `limits`. Allocates nothing until the
    /// first bytes arrive.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Decoder, Framing, Limits};
    ///
    /// let decoder = Decoder::new(Framing::Newline, &Limits::new());
    /// assert_eq!(decoder.buffered(), 0);
    /// ```
    #[must_use]
    pub fn new(framing: Framing, limits: &Limits) -> Self {
        Self {
            framing,
            max_message: limits.max_message_bytes,
            max_header_bytes: limits.max_header_bytes,
            max_headers: limits.max_headers,
            buf: Vec::new(),
            start: 0,
            end: 0,
            scan: 0,
            state: Self::fresh(framing),
            drained: true,
            failed: None,
        }
    }

    /// The framing this decoder reads.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// assert_eq!(Framing::Newline.decoder(&Limits::new()).framing(), Framing::Newline);
    /// ```
    #[inline]
    #[must_use]
    pub const fn framing(&self) -> Framing {
        self.framing
    }

    /// Bytes buffered and not yet returned or skipped.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// let mut d = Framing::Newline.decoder(&Limits::new());
    /// d.feed(b"{\"partial\"");
    /// assert_eq!(d.next_frame(), Ok(None));
    /// assert_eq!(d.buffered(), 10);
    /// ```
    #[inline]
    #[must_use]
    pub const fn buffered(&self) -> usize {
        self.end - self.start
    }

    /// The most memory the buffer can ever hold under these limits.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits, READ_CHUNK};
    ///
    /// let limits = Limits::new().with_max_message_bytes(1000).with_max_header_bytes(100);
    /// assert_eq!(Framing::Newline.decoder(&limits).memory_bound(), 1001 + READ_CHUNK);
    /// ```
    #[must_use]
    pub const fn memory_bound(&self) -> usize {
        let message = self.max_message.saturating_add(1);
        let largest = if message > self.max_header_bytes {
            message
        } else {
            self.max_header_bytes
        };
        largest.saturating_add(READ_CHUNK)
    }

    /// Where to put the next bytes read from the stream; follow with
    /// [`commit`](Decoder::commit).
    ///
    /// Empty until [`next_frame`](Decoder::next_frame) has returned
    /// `Ok(None)` (every complete message taken), and after a fatal error.
    /// The slice is at least [`READ_CHUNK`] bytes otherwise, more while a large
    /// body arrives.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// let mut d = Framing::Newline.decoder(&Limits::new());
    /// let space = d.space();
    /// space[..3].copy_from_slice(b"[]\n");
    /// d.commit(3);
    /// assert!(d.space().is_empty()); // a message may be ready: take it first
    /// assert_eq!(d.next_frame(), Ok(Some(&b"[]"[..])));
    /// assert_eq!(d.next_frame(), Ok(None));
    /// assert!(!d.space().is_empty());
    /// ```
    pub fn space(&mut self) -> &mut [u8] {
        if !self.drained || self.failed.is_some() {
            return &mut [];
        }
        if self.start == self.end {
            self.start = 0;
            self.end = 0;
            self.scan = 0;
        }
        let want = match self.state {
            State::Body { length } => {
                let have = self.end - self.start;
                let need = length.saturating_sub(have);
                // Grow as the body arrives (doubling), never to the announced
                // length on the header's word alone.
                if need <= READ_CHUNK {
                    READ_CHUNK
                } else {
                    need.min(have.max(READ_CHUNK))
                }
            }
            _ => READ_CHUNK,
        };
        if self.buf.len() - self.end < want {
            if self.start > 0 {
                self.buf.copy_within(self.start..self.end, 0);
                self.end -= self.start;
                self.scan -= self.start;
                self.start = 0;
            }
            let missing = want.saturating_sub(self.buf.len() - self.end);
            if missing > 0 {
                self.buf.reserve_exact(missing);
                self.buf.resize(self.buf.len() + missing, 0);
            }
        }
        &mut self.buf[self.end..]
    }

    /// Records that `n` bytes were written at the start of the last
    /// [`space`](Decoder::space). Values beyond that space are clamped.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// let mut d = Framing::Newline.decoder(&Limits::new());
    /// d.space()[..2].copy_from_slice(b"1\n");
    /// d.commit(2);
    /// assert_eq!(d.next_frame(), Ok(Some(&b"1"[..])));
    /// ```
    pub fn commit(&mut self, n: usize) {
        let n = n.min(self.buf.len() - self.end);
        if n > 0 {
            self.end += n;
            self.drained = false;
        }
    }

    /// Copies as much of `bytes` as the decoder takes now and returns how many
    /// it took: zero until [`next_frame`](Decoder::next_frame) has returned
    /// `Ok(None)`. Feed the rest after taking the messages out.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{Framing, Limits};
    ///
    /// let mut d = Framing::Newline.decoder(&Limits::new());
    /// let mut input: &[u8] = b"1\n2\n3\n";
    /// let mut bodies = Vec::new();
    /// while !input.is_empty() {
    ///     let n = d.feed(input);
    ///     input = &input[n..];
    ///     while let Some(body) = d.next_frame()? {
    ///         bodies.push(body.to_vec());
    ///     }
    /// }
    /// assert_eq!(bodies, [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]);
    /// # Ok::<(), rpc_io::FrameError>(())
    /// ```
    pub fn feed(&mut self, bytes: &[u8]) -> usize {
        let space = self.space();
        let n = bytes.len().min(space.len());
        space[..n].copy_from_slice(&bytes[..n]);
        self.commit(n);
        n
    }

    /// The next complete message body, `Ok(None)` when more bytes are needed.
    ///
    /// # Errors
    ///
    /// A [`FrameError`]. After [`TooLarge`](FrameError::TooLarge) call again:
    /// the decoder has moved on. Any other error is final and returned again
    /// by every later call.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{FrameError, Framing, Limits};
    ///
    /// let mut d = Framing::ContentLength.decoder(&Limits::new().with_max_message_bytes(3));
    /// d.feed(b"Content-Length: 5\r\n\r\nhelloContent-Length: 2\r\n\r\nok");
    /// assert_eq!(d.next_frame(), Err(FrameError::TooLarge { length: Some(5), limit: 3 }));
    /// assert_eq!(d.next_frame(), Ok(Some(&b"ok"[..])));
    /// ```
    pub fn next_frame(&mut self) -> Result<Option<&[u8]>, FrameError> {
        match self.next_range() {
            Ok(Some(range)) => Ok(Some(&self.buf[range])),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Ends the stream: call when the input is exhausted, after
    /// [`next_frame`](Decoder::next_frame) has returned `Ok(None)`.
    ///
    /// Returns a final newline-delimited message that had no `\n` (the end of
    /// the stream ends the line), and `Ok(None)` when the stream ended
    /// cleanly between messages. Call until it returns `Ok(None)` or an error.
    ///
    /// # Errors
    ///
    /// [`FrameError::Truncated`] if the stream ended inside a Content-Length
    /// message (headers or body); any error `next_frame` would return.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{FrameError, Framing, Limits};
    ///
    /// let mut d = Framing::Newline.decoder(&Limits::new());
    /// d.feed(b"{\"last\":true}");
    /// assert_eq!(d.next_frame(), Ok(None));
    /// assert_eq!(d.finish(), Ok(Some(&b"{\"last\":true}"[..])));
    /// assert_eq!(d.finish(), Ok(None));
    ///
    /// let mut d = Framing::ContentLength.decoder(&Limits::new());
    /// d.feed(b"Content-Length: 9\r\n\r\n{}");
    /// assert_eq!(d.next_frame(), Ok(None));
    /// assert_eq!(d.finish(), Err(FrameError::Truncated));
    /// ```
    pub fn finish(&mut self) -> Result<Option<&[u8]>, FrameError> {
        match self.finish_range() {
            Ok(Some(range)) => Ok(Some(&self.buf[range])),
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn fresh(framing: Framing) -> State {
        match framing {
            Framing::ContentLength => State::Headers {
                lines: 0,
                bytes: 0,
                length: None,
            },
            Framing::Newline => State::Line,
        }
    }

    fn fail(&mut self, error: FrameError) -> FrameError {
        self.failed = Some(error);
        error
    }

    fn finish_range(&mut self) -> Result<Option<Range<usize>>, FrameError> {
        if let Some(range) = self.next_range()? {
            return Ok(Some(range));
        }
        let rest = self.start..self.end;
        match self.state {
            State::Headers { lines: 0, .. } if is_blank(&self.buf[rest.clone()]) => {
                self.start = self.end;
                Ok(None)
            }
            State::Headers { .. } | State::Body { .. } | State::Skip { .. } => {
                Err(self.fail(FrameError::Truncated))
            }
            State::Line => {
                self.start = self.end;
                self.scan = self.end;
                let line = trim_line(&self.buf, rest);
                if line.len() > self.max_message {
                    Err(self.line_too_large())
                } else if is_blank(&self.buf[line.clone()]) {
                    Ok(None)
                } else {
                    Ok(Some(line))
                }
            }
            State::Discard => {
                self.start = self.end;
                self.scan = self.end;
                Ok(None)
            }
        }
    }

    fn next_range(&mut self) -> Result<Option<Range<usize>>, FrameError> {
        if let Some(error) = self.failed {
            return Err(error);
        }
        loop {
            match self.state {
                State::Headers {
                    lines,
                    bytes,
                    length,
                } => {
                    let Some(at) = find_newline(&self.buf[self.scan..self.end]) else {
                        self.scan = self.end;
                        let pending = self.end - self.start;
                        if bytes.saturating_add(pending) > self.max_header_bytes {
                            return Err(self.fail(FrameError::HeaderTooLarge {
                                limit: self.max_header_bytes,
                            }));
                        }
                        return Ok(self.wait());
                    };
                    let line_end = self.scan + at + 1;
                    let line_len = line_end - self.start;
                    let line = trim_line(&self.buf, self.start..line_end - 1);
                    if line.is_empty() {
                        self.start = line_end;
                        self.scan = line_end;
                        if lines == 0 {
                            // Stray blank lines between messages are skipped.
                            continue;
                        }
                        self.end_headers(length)?;
                        continue;
                    }
                    let bytes = bytes.saturating_add(line_len);
                    if bytes > self.max_header_bytes {
                        return Err(self.fail(FrameError::HeaderTooLarge {
                            limit: self.max_header_bytes,
                        }));
                    }
                    let lines = lines + 1;
                    if lines > self.max_headers {
                        return Err(self.fail(FrameError::TooManyHeaders {
                            limit: self.max_headers,
                        }));
                    }
                    let length = match parse_header(&self.buf[line]) {
                        Header::ContentLength(_) if length.is_some() => {
                            return Err(self.fail(FrameError::DuplicateContentLength));
                        }
                        Header::ContentLength(n) => Some(n),
                        Header::Other => length,
                        Header::Invalid(error) => return Err(self.fail(error)),
                    };
                    self.start = line_end;
                    self.scan = line_end;
                    self.state = State::Headers {
                        lines,
                        bytes,
                        length,
                    };
                }
                State::Body { length } => {
                    if self.end - self.start < length {
                        return Ok(self.wait());
                    }
                    let body = self.start..self.start + length;
                    self.start += length;
                    self.scan = self.start;
                    self.state = Self::fresh(self.framing);
                    return Ok(Some(body));
                }
                State::Skip { remaining } => {
                    let available = (self.end - self.start) as u64;
                    let take = available.min(remaining);
                    // `take` is at most `available`, a `usize`.
                    self.start += take as usize;
                    self.scan = self.start;
                    if take == remaining {
                        self.state = Self::fresh(self.framing);
                        continue;
                    }
                    self.state = State::Skip {
                        remaining: remaining - take,
                    };
                    return Ok(self.wait());
                }
                State::Line => {
                    let Some(at) = find_newline(&self.buf[self.scan..self.end]) else {
                        self.scan = self.end;
                        // The line so far, plus an `\r` that may precede its end.
                        if self.end - self.start > self.max_message.saturating_add(1) {
                            self.start = self.end;
                            self.scan = self.end;
                            self.state = State::Discard;
                            return Err(self.line_too_large());
                        }
                        return Ok(self.wait());
                    };
                    let newline = self.scan + at;
                    let line = trim_line(&self.buf, self.start..newline);
                    self.start = newline + 1;
                    self.scan = self.start;
                    // Length before blankness, and no length in the error: a
                    // line that arrives in pieces is judged on its length
                    // alone (above), so a whole one must be too, or the
                    // result would depend on how the input was split.
                    if line.len() > self.max_message {
                        return Err(self.line_too_large());
                    }
                    if is_blank(&self.buf[line.clone()]) {
                        continue;
                    }
                    return Ok(Some(line));
                }
                State::Discard => match find_newline(&self.buf[self.scan..self.end]) {
                    Some(at) => {
                        self.start = self.scan + at + 1;
                        self.scan = self.start;
                        self.state = State::Line;
                    }
                    None => {
                        self.start = self.end;
                        self.scan = self.end;
                        return Ok(self.wait());
                    }
                },
            }
        }
    }

    /// Ends a header section: the body comes next, or is skipped when too
    /// large.
    fn end_headers(&mut self, length: Option<u64>) -> Result<(), FrameError> {
        let Some(length) = length else {
            return Err(self.fail(FrameError::MissingContentLength));
        };
        match usize::try_from(length) {
            Ok(n) if n <= self.max_message => {
                self.state = State::Body { length: n };
                Ok(())
            }
            _ => {
                self.state = State::Skip { remaining: length };
                Err(FrameError::TooLarge {
                    length: Some(length),
                    limit: self.max_message,
                })
            }
        }
    }

    /// The error for a newline-delimited line over the limit. Its length is
    /// not given: a line cut off before its end has no known length, and
    /// reporting it only when the end happened to be buffered would make the
    /// error depend on how the input was split.
    fn line_too_large(&self) -> FrameError {
        FrameError::TooLarge {
            length: None,
            limit: self.max_message,
        }
    }

    /// Nothing complete is buffered: more bytes may be taken.
    fn wait<T>(&mut self) -> Option<T> {
        self.drained = true;
        None
    }
}

/// One parsed header line.
enum Header {
    ContentLength(u64),
    Other,
    Invalid(FrameError),
}

fn parse_header(line: &[u8]) -> Header {
    let Some(colon) = line.iter().position(|&b| b == b':') else {
        return Header::Invalid(FrameError::InvalidHeader);
    };
    let (name, value) = line.split_at(colon);
    // RFC 9110 field names are tokens. Anything else (a space, a brace) is a
    // body that ran into the headers, not a header to skip over.
    if name.is_empty() || !name.iter().all(|&b| is_token(b)) {
        return Header::Invalid(FrameError::InvalidHeader);
    }
    if !name.eq_ignore_ascii_case(b"content-length") {
        return Header::Other;
    }
    let value = trim_spaces(&value[1..]);
    if value.is_empty() {
        return Header::Invalid(FrameError::InvalidContentLength);
    }
    let mut n: u64 = 0;
    for &b in value {
        if !b.is_ascii_digit() {
            return Header::Invalid(FrameError::InvalidContentLength);
        }
        let Some(next) = n
            .checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(b - b'0')))
        else {
            return Header::Invalid(FrameError::InvalidContentLength);
        };
        n = next;
    }
    Header::ContentLength(n)
}

/// Whether `b` may appear in an HTTP field name (RFC 9110 `tchar`).
fn is_token(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// `bytes` without leading and trailing spaces and tabs.
fn trim_spaces(bytes: &[u8]) -> &[u8] {
    let start = bytes.iter().position(|&b| b != b' ' && b != b'\t');
    let end = bytes.iter().rposition(|&b| b != b' ' && b != b'\t');
    match (start, end) {
        (Some(start), Some(end)) => &bytes[start..=end],
        _ => &[],
    }
}

/// `range` without a trailing `\r`.
fn trim_line(buf: &[u8], range: Range<usize>) -> Range<usize> {
    if range.end > range.start && buf[range.end - 1] == b'\r' {
        range.start..range.end - 1
    } else {
        range
    }
}

/// Whether `bytes` holds only JSON whitespace.
fn is_blank(bytes: &[u8]) -> bool {
    bytes
        .iter()
        .all(|&b| matches!(b, b' ' | b'\t' | b'\r' | b'\n'))
}

const ONES: u64 = 0x0101_0101_0101_0101;
const HIGHS: u64 = 0x8080_8080_8080_8080;

/// Whether any byte of `word` is zero.
#[inline]
const fn has_zero(word: u64) -> bool {
    word.wrapping_sub(ONES) & !word & HIGHS != 0
}

/// The first byte of `hay` that `hit` matches, eight bytes at a time where a
/// word-wide test can rule a chunk out.
#[inline]
fn find_with(
    hay: &[u8],
    word_hit: impl Fn(u64) -> bool,
    hit: impl Fn(u8) -> bool,
) -> Option<usize> {
    let mut chunks = hay.chunks_exact(8);
    let mut base = 0;
    for chunk in &mut chunks {
        let word = u64::from_le_bytes([
            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
        ]);
        if word_hit(word) {
            return chunk.iter().position(|&b| hit(b)).map(|i| base + i);
        }
        base += 8;
    }
    chunks
        .remainder()
        .iter()
        .position(|&b| hit(b))
        .map(|i| base + i)
}

/// The first `\n` in `hay`.
pub(crate) fn find_newline(hay: &[u8]) -> Option<usize> {
    find_with(
        hay,
        |w| has_zero(w ^ (ONES * u64::from(b'\n'))),
        |b| b == b'\n',
    )
}

/// The first `\n` or `\r` in `hay`.
fn find_line_break(hay: &[u8]) -> Option<usize> {
    find_with(
        hay,
        |w| has_zero(w ^ (ONES * u64::from(b'\n'))) || has_zero(w ^ (ONES * u64::from(b'\r'))),
        |b| b == b'\n' || b == b'\r',
    )
}

/// The decimal digits of `n`, right-aligned, and where they start.
fn decimal(mut n: u64) -> ([u8; 20], usize) {
    let mut digits = [0u8; 20];
    let mut at = digits.len();
    loop {
        at -= 1;
        // `n % 10` is below 10, so the cast cannot truncate.
        digits[at] = b'0' + (n % 10) as u8;
        n /= 10;
        if n == 0 {
            break;
        }
    }
    (digits, at)
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::vec;

    use proptest::prelude::*;

    use super::*;

    fn frames(
        framing: Framing,
        limits: &Limits,
        input: &[u8],
        chunk: usize,
    ) -> Vec<Result<Vec<u8>, FrameError>> {
        let mut d = framing.decoder(limits);
        let mut out = Vec::new();
        let mut rest = input;
        loop {
            loop {
                match d.next_frame() {
                    Ok(Some(f)) => out.push(Ok(f.to_vec())),
                    Ok(None) => break,
                    Err(e) => {
                        out.push(Err(e));
                        if !e.is_recoverable() {
                            return out;
                        }
                    }
                }
                assert!(d.buf.len() <= d.memory_bound());
            }
            if rest.is_empty() {
                break;
            }
            let take = rest.len().min(chunk.max(1));
            let n = d.feed(&rest[..take]);
            rest = &rest[n..];
            assert!(
                d.buf.len() <= d.memory_bound(),
                "{} > {}",
                d.buf.len(),
                d.memory_bound()
            );
        }
        loop {
            match d.finish() {
                Ok(Some(f)) => out.push(Ok(f.to_vec())),
                Ok(None) => break,
                Err(e) => {
                    out.push(Err(e));
                    if !e.is_recoverable() {
                        break;
                    }
                }
            }
        }
        out
    }

    #[test]
    fn test_content_length_basic() {
        let input = b"\r\nContent-Length: 2\r\ncontent-type: x\r\n\r\n{}content-length:3\n\n[1]";
        let got = frames(Framing::ContentLength, &Limits::new(), input, 3);
        assert_eq!(got, vec![Ok(b"{}".to_vec()), Ok(b"[1]".to_vec())]);
    }

    #[test]
    fn test_content_length_errors() {
        let limits = Limits::new().with_max_headers(2).with_max_header_bytes(64);
        for (input, error) in [
            (
                &b"Content-Length: -1\r\n\r\n"[..],
                FrameError::InvalidContentLength,
            ),
            (
                b"Content-Length: \r\n\r\n",
                FrameError::InvalidContentLength,
            ),
            (
                b"Content-Length: 99999999999999999999\r\n\r\n",
                FrameError::InvalidContentLength,
            ),
            (
                b"Content-Length: 1 2\r\n\r\n",
                FrameError::InvalidContentLength,
            ),
            (
                b"Content-Length: +1\r\n\r\n",
                FrameError::InvalidContentLength,
            ),
            (b"X: 1\r\n\r\n", FrameError::MissingContentLength),
            (
                b"Content-Length: 1\r\nContent-Length: 1\r\n\r\n",
                FrameError::DuplicateContentLength,
            ),
            (b"nocolon\r\n\r\n", FrameError::InvalidHeader),
            (
                b"A: 1\r\nB: 2\r\nC: 3\r\n",
                FrameError::TooManyHeaders { limit: 2 },
            ),
            (&[b'x'; 100][..], FrameError::HeaderTooLarge { limit: 64 }),
            (b"Content-Length: 5\r\n\r\nab", FrameError::Truncated),
            (b"Content-Len", FrameError::Truncated),
        ] {
            let got = frames(Framing::ContentLength, &limits, input, 7);
            assert_eq!(
                got.last(),
                Some(&Err(error)),
                "{:?}",
                core::str::from_utf8(input)
            );
        }
    }

    #[test]
    fn test_content_length_overflowing_u64_is_invalid_not_large() {
        let got = frames(
            Framing::ContentLength,
            &Limits::new(),
            b"Content-Length: 18446744073709551616\r\n\r\n",
            64,
        );
        assert_eq!(got, vec![Err(FrameError::InvalidContentLength)]);
        let got = frames(
            Framing::ContentLength,
            &Limits::new(),
            b"Content-Length: 18446744073709551615\r\n\r\n",
            64,
        );
        assert_eq!(
            got,
            vec![
                Err(FrameError::TooLarge {
                    length: Some(u64::MAX),
                    limit: 64 * 1024 * 1024
                }),
                Err(FrameError::Truncated)
            ]
        );
    }

    #[test]
    fn test_oversized_body_is_skipped_without_buffering() {
        let limits = Limits::new().with_max_message_bytes(4);
        let mut input = b"Content-Length: 100000\r\n\r\n".to_vec();
        input.extend(core::iter::repeat_n(b'x', 100_000));
        input.extend_from_slice(b"Content-Length: 2\r\n\r\nok");
        let got = frames(Framing::ContentLength, &limits, &input, 4096);
        assert_eq!(
            got,
            vec![
                Err(FrameError::TooLarge {
                    length: Some(100_000),
                    limit: 4
                }),
                Ok(b"ok".to_vec())
            ]
        );
    }

    #[test]
    fn test_newline_basic() {
        let input = b"\n  \r\n{\"a\":1}\r\n[2]\n\n3";
        let got = frames(Framing::Newline, &Limits::new(), input, 2);
        assert_eq!(
            got,
            vec![
                Ok(b"{\"a\":1}".to_vec()),
                Ok(b"[2]".to_vec()),
                Ok(b"3".to_vec())
            ]
        );
    }

    #[test]
    fn test_newline_oversized_line_is_skipped() {
        let limits = Limits::new().with_max_message_bytes(4);
        let mut input = vec![b'y'; 50_000];
        input.extend_from_slice(b"\nok\n12345\n");
        let got = frames(Framing::Newline, &limits, &input, 1000);
        assert_eq!(
            got,
            vec![
                Err(FrameError::TooLarge {
                    length: None,
                    limit: 4
                }),
                Ok(b"ok".to_vec()),
                Err(FrameError::TooLarge {
                    length: None,
                    limit: 4
                }),
            ]
        );
    }

    #[test]
    fn test_body_reserve_grows_with_arrival() {
        let mut d = Framing::ContentLength.decoder(&Limits::new());
        let _taken = d.feed(b"Content-Length: 60000000\r\n\r\n");
        assert_eq!(d.next_frame(), Ok(None));
        // The header alone does not reserve 60 MB.
        assert!(d.space().len() <= 2 * READ_CHUNK);
    }

    #[test]
    fn test_encode_newline_replaces_breaks() {
        let mut out = Vec::new();
        Framing::Newline.encode(b"{\r\n\"a\":\n1}", &mut out);
        assert_eq!(out, b"{  \"a\": 1}\n");
    }

    #[test]
    fn test_find_newline_matches_naive() {
        let mut hay = vec![b'a'; 40];
        assert_eq!(find_newline(&hay), None);
        for at in 0..40 {
            hay[at] = b'\n';
            assert_eq!(find_newline(&hay), Some(at));
            hay[at] = 0x8a; // high bit set, not a newline
            assert_eq!(find_newline(&hay), None);
            hay[at] = b'a';
        }
    }

    proptest! {
        #[test]
        fn prop_chunking_does_not_change_frames(
            input in proptest::collection::vec(prop_oneof![Just(b'\n'), Just(b'\r'), Just(b':'), Just(b'7'), any::<u8>()], 0..600),
            a in 1usize..64,
            b in 1usize..64,
            newline in any::<bool>(),
        ) {
            let framing = if newline { Framing::Newline } else { Framing::ContentLength };
            let limits = Limits::new().with_max_message_bytes(40).with_max_header_bytes(48).with_max_headers(3);
            prop_assert_eq!(frames(framing, &limits, &input, a), frames(framing, &limits, &input, b));
        }
    }
}
