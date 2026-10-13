//! [`Limits`]: the budgets every incoming message is held to.

use jsonx_lib::Parser;

/// Budgets on what a peer can make the crate hold or do.
///
/// Every byte a peer sends passes these checks before it costs memory: the
/// framings refuse oversized headers and bodies before buffering them, and
/// message decoding runs the JSON parser with `json`'s budgets (its
/// `max_input_bytes` is replaced by `max_message_bytes`).
///
/// | Budget | Default | Bounds |
/// |---|---:|---|
/// | `max_message_bytes` | 64 MiB | one message body (a Content-Length body or a line) |
/// | `max_header_bytes` | 8 KiB | one Content-Length header section, terminators included |
/// | `max_headers` | 32 | header lines in one header section |
/// | `max_batch` | 1024 | messages in one batch |
/// | `max_skip_bytes` | `u64::MAX` | the announced length of an oversized Content-Length message that is still skipped; above it the connection is closed |
/// | `json` | `jsonx_lib::Limits::new()` | depth 128, 16 MiB strings, 1 KiB numbers, 4 Mi values |
///
/// `#[non_exhaustive]`: start from [`Limits::new`] and chain the `with_*`
/// methods, or assign the public fields of a `Limits::new()`.
///
/// # Examples
///
/// ```
/// use rpc_io::Limits;
///
/// const SMALL: Limits = Limits::new().with_max_message_bytes(1 << 20).with_max_batch(16);
/// assert_eq!(SMALL.max_message_bytes, 1 << 20);
/// assert_eq!(SMALL.max_headers, 32);
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Limits {
    /// Largest message body, in bytes.
    pub max_message_bytes: usize,
    /// Largest Content-Length header section, in bytes, terminators included.
    pub max_header_bytes: usize,
    /// Most header lines in one Content-Length header section.
    pub max_headers: usize,
    /// Most messages in one batch; `0` turns batches off, so any batch is
    /// refused at its `[` (MCP since the 2025-06-18 revision has no
    /// batching).
    pub max_batch: usize,
    /// The largest announced Content-Length the decoder still skips when it
    /// is over `max_message_bytes`. A larger one is a fatal
    /// [`FrameError::TooLargeToSkip`](crate::FrameError::TooLargeToSkip):
    /// a peer that announces a huge body would otherwise keep the connection
    /// discarding its input for as long as it likes.
    pub max_skip_bytes: u64,
    /// The JSON parser's budgets (depth, string and number length, value
    /// count). Its `max_input_bytes` is ignored: `max_message_bytes` applies.
    pub json: jsonx_lib::Limits,
}

impl Limits {
    /// The defaults in the table above.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().max_batch, 1024);
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_message_bytes: 64 * 1024 * 1024,
            max_header_bytes: 8 * 1024,
            max_headers: 32,
            max_batch: 1024,
            max_skip_bytes: u64::MAX,
            json: jsonx_lib::Limits::new(),
        }
    }

    /// Turns batches off (`max_batch` 0): any batch is answered with one
    /// `INVALID_REQUEST`, "batches are not accepted", without reading it.
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, ErrorCode, Incoming, Limits};
    ///
    /// let limits = Limits::new().without_batches();
    /// let Incoming::Single(Err(refused)) = decode(br#"[{"jsonrpc":"2.0","method":"x"}]"#, &limits) else {
    ///     panic!("a batch was accepted");
    /// };
    /// assert_eq!(refused.error().code(), ErrorCode::INVALID_REQUEST);
    /// assert_eq!(refused.error().message(), "batches are not accepted");
    /// ```
    #[must_use]
    pub const fn without_batches(self) -> Self {
        self.with_max_batch(0)
    }

    /// Sets `max_message_bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().with_max_message_bytes(10).max_message_bytes, 10);
    /// ```
    #[must_use]
    pub const fn with_max_message_bytes(mut self, bytes: usize) -> Self {
        self.max_message_bytes = bytes;
        self
    }

    /// Sets `max_header_bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().with_max_header_bytes(512).max_header_bytes, 512);
    /// ```
    #[must_use]
    pub const fn with_max_header_bytes(mut self, bytes: usize) -> Self {
        self.max_header_bytes = bytes;
        self
    }

    /// Sets `max_headers`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().with_max_headers(4).max_headers, 4);
    /// ```
    #[must_use]
    pub const fn with_max_headers(mut self, count: usize) -> Self {
        self.max_headers = count;
        self
    }

    /// Sets `max_batch`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().with_max_batch(8).max_batch, 8);
    /// ```
    #[must_use]
    pub const fn with_max_batch(mut self, count: usize) -> Self {
        self.max_batch = count;
        self
    }

    /// Sets `max_skip_bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::new().with_max_skip_bytes(1 << 30).max_skip_bytes, 1 << 30);
    /// ```
    #[must_use]
    pub const fn with_max_skip_bytes(mut self, bytes: u64) -> Self {
        self.max_skip_bytes = bytes;
        self
    }

    /// Sets the JSON parser's budgets.
    ///
    /// # Examples
    ///
    /// ```
    /// let limits = rpc_io::Limits::new().with_json(jsonx_lib::Limits::new().with_max_depth(16));
    /// assert_eq!(limits.json.max_depth, 16);
    /// ```
    #[must_use]
    pub const fn with_json(mut self, json: jsonx_lib::Limits) -> Self {
        self.json = json;
        self
    }

    /// The JSON parser these limits configure: `json`'s budgets with
    /// `max_message_bytes` as the input size, duplicate keys refused.
    ///
    /// # Examples
    ///
    /// ```
    /// let parser = rpc_io::Limits::new().with_max_message_bytes(100).parser();
    /// assert_eq!(parser.get_limits().max_input_bytes, 100);
    /// ```
    pub const fn parser(&self) -> Parser {
        Parser::new().limits(self.json.with_max_input_bytes(self.max_message_bytes))
    }
}

impl Default for Limits {
    fn default() -> Self {
        Self::new()
    }
}
