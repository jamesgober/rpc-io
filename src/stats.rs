//! [`Stats`]: what one connection has done, counted as it happens.

/// Counters and gauges for one connection: traffic both ways, the outcomes of
/// our requests, and what is outstanding now.
///
/// The endpoint keeps them as plain integers under the lock it takes for
/// every message anyway, so counting costs a few additions per message and
/// no atomics; [`Endpoint::stats`](crate::Endpoint::stats) and
/// [`Peer::stats`](crate::Peer::stats) take a snapshot. Counters start at 0
/// and only grow (they wrap after 2^64); gauges are the value at the
/// snapshot.
///
/// | Field | Kind | Counts | Normal | Worth a look |
/// |---|---|---|---|---|
/// | `bodies_in`, `bytes_in` | counter | message bodies handled (a batch counts once) and their bytes, framing excluded | grows with traffic | `bytes_in / bodies_in` near `max_message_bytes` |
/// | `requests_in`, `notifications_in`, `responses_in` | counter | messages from the peer, batch elements included | | |
/// | `invalid_in` | counter | malformed messages and messages skipped as oversized | 0 | any growth: a broken or hostile peer |
/// | `bodies_out`, `bytes_out` | counter | message bodies sent (a batch counts once) and their bytes | | |
/// | `requests_out`, `notifications_out`, `responses_out` | counter | messages sent, batch elements included | | |
/// | `errors_out` | counter | error responses sent, of `responses_out` | low | a large share: handlers failing |
/// | `timed_out`, `evicted`, `cancelled_out` | counter | our requests that ended so | 0 | timeouts: a slow or stuck peer; evictions: raise `max_pending` |
/// | `cancelled_in` | counter | the peer's requests it cancelled | | |
/// | `panics` | counter | handler panics contained | 0 | any: a bug in a handler |
/// | `pending` | gauge | our requests awaiting an answer | small | near `max_pending` |
/// | `in_flight` | gauge | deferred requests being answered | | near `max_in_flight` |
/// | `held` | gauge | messages held until the gate opens | 0 once open | |
///
/// # Examples
///
/// ```
/// use rpc_io::{Endpoint, Router};
///
/// let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
/// let (tx, _rx) = std::sync::mpsc::channel::<String>();
/// let mut endpoint = Endpoint::new(tx, router, ());
/// endpoint.handle(br#"[{"jsonrpc":"2.0","id":1,"method":"ping"},{"jsonrpc":"2.0","id":2,"method":"nope"}]"#)?;
/// let stats = endpoint.stats();
/// assert_eq!((stats.bodies_in, stats.requests_in), (1, 2));
/// assert_eq!((stats.bodies_out, stats.responses_out, stats.errors_out), (1, 2, 1));
/// # Ok::<(), rpc_io::Error>(())
/// ```
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct Stats {
    /// Message bodies handled; a batch counts once.
    pub bodies_in: u64,
    /// Bytes of the message bodies handled, framing excluded.
    pub bytes_in: u64,
    /// Requests from the peer, batch elements included.
    pub requests_in: u64,
    /// Notifications from the peer, batch elements included.
    pub notifications_in: u64,
    /// Responses from the peer, matched to one of our requests or not.
    pub responses_in: u64,
    /// Malformed messages, and messages skipped as oversized.
    pub invalid_in: u64,
    /// Message bodies sent; a batch counts once.
    pub bodies_out: u64,
    /// Bytes of the message bodies sent, framing excluded.
    pub bytes_out: u64,
    /// Requests sent.
    pub requests_out: u64,
    /// Notifications sent, cancel notifications included.
    pub notifications_out: u64,
    /// Responses sent, results and errors, batch elements included.
    pub responses_out: u64,
    /// Error responses sent (of `responses_out`).
    pub errors_out: u64,
    /// Our requests that timed out.
    pub timed_out: u64,
    /// Our requests evicted over `max_pending`.
    pub evicted: u64,
    /// Our requests cancelled by this side.
    pub cancelled_out: u64,
    /// The peer's requests the peer cancelled while they were in flight.
    pub cancelled_in: u64,
    /// Handler panics contained.
    pub panics: u64,
    /// Our requests awaiting an answer now.
    pub pending: usize,
    /// Deferred requests being answered now.
    pub in_flight: usize,
    /// Messages held now until the gate opens.
    pub held: usize,
}

/// What one incoming body held, counted before its turn.
#[derive(Clone, Copy, Default)]
pub(crate) struct Tally {
    /// Whether it was a body (not a message the framing skipped).
    pub(crate) body: bool,
    pub(crate) bytes: usize,
    pub(crate) requests: u64,
    pub(crate) notifications: u64,
    pub(crate) responses: u64,
    pub(crate) invalid: u64,
}

impl Stats {
    /// Adds an incoming body.
    pub(crate) fn take_in(&mut self, tally: &Tally) {
        self.bodies_in = self.bodies_in.wrapping_add(u64::from(tally.body));
        self.bytes_in = self.bytes_in.wrapping_add(tally.bytes as u64);
        self.requests_in = self.requests_in.wrapping_add(tally.requests);
        self.notifications_in = self.notifications_in.wrapping_add(tally.notifications);
        self.responses_in = self.responses_in.wrapping_add(tally.responses);
        self.invalid_in = self.invalid_in.wrapping_add(tally.invalid);
    }
}

/// Adds one to a counter.
pub(crate) fn bump(counter: &mut u64) {
    *counter = counter.wrapping_add(1);
}
