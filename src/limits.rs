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
/// The defaults suit a local peer over stdio (an editor, an agent host).
/// Every message is parsed under these budgets, and a handler that calls
/// [`Params::parse`](crate::Params) builds a full value tree within them, so
/// one 64 MiB array of small numbers can cost several times its size in
/// memory. For peers across a network use [`Limits::network`] or
/// [`Limits::strict`].
///
/// | Preset | Messages | Headers | Batch | Skip | JSON depth / strings / values |
/// |---|---:|---:|---:|---:|---|
/// | [`new`](Limits::new), [`lsp`](Limits::lsp) | 64 MiB | 8 KiB, 32 | 1024 | any | 128 / 16 MiB / 4 Mi |
/// | [`mcp`](Limits::mcp) | 64 MiB | 8 KiB, 32 | off | any | 128 / 16 MiB / 4 Mi |
/// | [`network`](Limits::network) | 4 MiB | 4 KiB, 16 | 128 | 64 MiB | 64 / 1 MiB / 256 Ki |
/// | [`strict`](Limits::strict) | 1 MiB | 2 KiB, 8 | 16 | 4 MiB | 32 / 256 KiB / 64 Ki |
///
/// One field is a policy rather than a budget: `lenient_ids` (off in every
/// preset) accepts the request ids JSON-RPC 2.0 allows but LSP and MCP
/// forbid; see [`Limits::with_lenient_ids`].
///
/// `#[non_exhaustive]`: start from [`Limits::new`] (or a preset) and chain
/// the `with_*` methods, or assign the public fields of a `Limits::new()`.
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
    /// Whether a request id may be `null` or any JSON number (a fraction, an
    /// exponent, an integer outside `i64`), as JSON-RPC 2.0 allows; off, such
    /// a request is refused as invalid, as LSP and MCP require. See
    /// [`Limits::with_lenient_ids`].
    pub lenient_ids: bool,
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
            lenient_ids: false,
        }
    }

    /// LSP over stdio: the defaults. Editors send whole files (`didOpen`),
    /// so messages may be large; batches are allowed, though LSP clients do
    /// not send them.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::lsp(), rpc_io::Limits::new());
    /// ```
    #[must_use]
    pub const fn lsp() -> Self {
        Self::new()
    }

    /// MCP over stdio: the defaults with batches off. MCP removed JSON-RPC
    /// batching in its 2025-06-18 revision; a peer on the 2025-03-26 revision
    /// may batch, and `with_max_batch` turns batches back on for it.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Limits::mcp().max_batch, 0);
    /// ```
    #[must_use]
    pub const fn mcp() -> Self {
        Self::new().without_batches()
    }

    /// A peer across a network (a TCP service, a remote MCP server): 4 MiB
    /// messages, 4 KiB header sections of at most 16 lines, batches of 128,
    /// a body over 64 MiB announced is not skipped but closes the
    /// connection, and the JSON budgets cut to depth 64, 1 MiB strings, 128
    /// byte numbers, and 256 Ki values.
    ///
    /// # Examples
    ///
    /// ```
    /// let limits = rpc_io::Limits::network();
    /// assert_eq!(limits.max_message_bytes, 4 << 20);
    /// assert_eq!(limits.json.max_depth, 64);
    /// ```
    #[must_use]
    pub const fn network() -> Self {
        Self {
            max_message_bytes: 4 * 1024 * 1024,
            max_header_bytes: 4 * 1024,
            max_headers: 16,
            max_batch: 128,
            max_skip_bytes: 64 * 1024 * 1024,
            json: jsonx_lib::Limits::new()
                .with_max_depth(64)
                .with_max_string_bytes(1024 * 1024)
                .with_max_number_bytes(128)
                .with_max_elements(256 * 1024),
            lenient_ids: false,
        }
    }

    /// An untrusted peer with small messages (an authentication step, a
    /// public endpoint): 1 MiB messages, 2 KiB header sections of at most 8
    /// lines, batches of 16, a body over 4 MiB announced closes the
    /// connection, and the JSON budgets cut to depth 32, 256 KiB strings, 64
    /// byte numbers, and 64 Ki values.
    ///
    /// # Examples
    ///
    /// ```
    /// let limits = rpc_io::Limits::strict();
    /// assert_eq!((limits.max_message_bytes, limits.max_batch), (1 << 20, 16));
    /// ```
    #[must_use]
    pub const fn strict() -> Self {
        Self {
            max_message_bytes: 1024 * 1024,
            max_header_bytes: 2 * 1024,
            max_headers: 8,
            max_batch: 16,
            max_skip_bytes: 4 * 1024 * 1024,
            json: jsonx_lib::Limits::new()
                .with_max_depth(32)
                .with_max_string_bytes(256 * 1024)
                .with_max_number_bytes(64)
                .with_max_elements(64 * 1024),
            lenient_ids: false,
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

    /// Accepts (`true`) or refuses (`false`, the default) the request ids
    /// JSON-RPC 2.0 allows and discourages: `null`, and numbers that are not
    /// integers within `i64`. Accepted, they are kept as [`Id::Null`] and
    /// [`Id::RawNumber`] and the response carries them back exactly as they
    /// were sent; a response from the peer may then carry a `RawNumber` id
    /// too (it matches none of ours, which are integers). For generic
    /// JSON-RPC services; LSP and MCP forbid such ids, and their presets
    /// leave this off.
    ///
    /// A `null` id names no one request: two `null`-id requests cannot both
    /// be in flight (the second is refused as a duplicate), and one cannot
    /// be cancelled.
    ///
    /// [`Id::Null`]: crate::Id::Null
    /// [`Id::RawNumber`]: crate::Id::RawNumber
    ///
    /// # Examples
    ///
    /// ```
    /// use rpc_io::{decode, Id, Incoming, Limits, Message};
    ///
    /// let body = br#"{"jsonrpc":"2.0","id":1.5,"method":"m"}"#;
    /// assert!(matches!(decode(body, &Limits::new()), Incoming::Single(Err(_))));
    /// let Incoming::Single(Ok(Message::Request(request))) = decode(body, &Limits::new().with_lenient_ids(true)) else {
    ///     panic!("refused");
    /// };
    /// assert_eq!(request.id, Id::RawNumber("1.5".into()));
    /// ```
    #[must_use]
    pub const fn with_lenient_ids(mut self, lenient: bool) -> Self {
        self.lenient_ids = lenient;
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
    /// let limits = rpc_io::Limits::new().with_json(rpc_io::jsonx::Limits::new().with_max_depth(16));
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
