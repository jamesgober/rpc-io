//! [`Pending`]: our own requests awaiting an answer, by id.

use alloc::collections::{BTreeMap, BTreeSet};
use core::time::Duration;

/// Outstanding outgoing requests: ids issued in order, each with a value (a
/// waiter, a callback, whatever the caller needs to deliver the answer to) and
/// an optional deadline.
///
/// Every operation is `O(log n)`:
///
/// - [`issue`](Pending::issue) assigns the next id; at the capacity, the
///   oldest request is evicted to make room and handed back, so a peer that
///   never answers cannot grow the map without bound;
/// - [`settle`](Pending::settle) removes a request when its answer arrives,
///   and returns `None` for an id that is not outstanding (forged, repeated,
///   or already timed out), so each request is delivered at most once;
/// - [`pop_expired`](Pending::pop_expired) removes requests whose deadline has
///   passed, earliest first.
///
/// Time is a `Duration` from an epoch the caller chooses (the endpoint uses
/// the moment it was created), so the map works without `std`.
///
/// Ids start at 1 (JavaScript peers that test an id for truthiness then never
/// see 0) and count up, so the smallest outstanding id is the oldest request.
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use rpc_io::Pending;
///
/// let mut pending = Pending::new(2);
/// let (a, _) = pending.issue("first", None);
/// let (b, _) = pending.issue("second", Some(Duration::from_secs(5)));
/// let (c, evicted) = pending.issue("third", None);
/// assert_eq!(evicted, Some((a, "first"))); // over capacity: the oldest goes
///
/// assert_eq!(pending.settle(c), Some("third"));
/// assert_eq!(pending.settle(c), None); // answered once only
///
/// assert_eq!(pending.pop_expired(Duration::from_secs(4)), None);
/// assert_eq!(pending.pop_expired(Duration::from_secs(5)), Some((b, "second")));
/// assert!(pending.is_empty());
/// ```
#[derive(Clone, Debug)]
pub struct Pending<T> {
    next: u64,
    capacity: usize,
    entries: BTreeMap<u64, Slot<T>>,
    /// `(deadline, id)` for every entry that has a deadline.
    deadlines: BTreeSet<(Duration, u64)>,
}

#[derive(Clone, Debug)]
struct Slot<T> {
    deadline: Option<Duration>,
    value: T,
}

impl<T> Pending<T> {
    /// An empty map that holds at most `capacity` requests (at least one).
    ///
    /// # Examples
    ///
    /// ```
    /// let pending: rpc_io::Pending<()> = rpc_io::Pending::new(1024);
    /// assert_eq!(pending.capacity(), 1024);
    /// ```
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            next: 1,
            capacity: capacity.max(1),
            entries: BTreeMap::new(),
            deadlines: BTreeSet::new(),
        }
    }

    /// The most requests held at once.
    ///
    /// # Examples
    ///
    /// ```
    /// assert_eq!(rpc_io::Pending::<u8>::new(0).capacity(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Requests outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(4);
    /// p.issue((), None);
    /// assert_eq!(p.len(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether no request is outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// assert!(rpc_io::Pending::<()>::new(4).is_empty());
    /// ```
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Whether `id` is outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(4);
    /// let (id, _) = p.issue((), None);
    /// assert!(p.contains(id));
    /// assert!(!p.contains(id + 1));
    /// ```
    #[inline]
    #[must_use]
    pub fn contains(&self, id: u64) -> bool {
        self.entries.contains_key(&id)
    }

    /// The value of outstanding request `id`.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(4);
    /// let (id, _) = p.issue("v", None);
    /// assert_eq!(p.get(id), Some(&"v"));
    /// ```
    #[must_use]
    pub fn get(&self, id: u64) -> Option<&T> {
        self.entries.get(&id).map(|slot| &slot.value)
    }

    /// Records a new request and returns its id. If the map was full, the
    /// oldest outstanding request is removed and returned with it: tell its
    /// waiter it will get no answer.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(1);
    /// let (first, none) = p.issue('a', None);
    /// assert_eq!((first, none), (1, None));
    /// let (second, evicted) = p.issue('b', None);
    /// assert_eq!((second, evicted), (2, Some((1, 'a'))));
    /// ```
    pub fn issue(&mut self, value: T, deadline: Option<Duration>) -> (u64, Option<(u64, T)>) {
        let evicted = if self.entries.len() >= self.capacity {
            self.entries.pop_first().map(|(id, slot)| {
                self.forget_deadline(id, slot.deadline);
                (id, slot.value)
            })
        } else {
            None
        };
        // Ids wrap only after 2^64 requests; even then, one still outstanding
        // is never reused.
        let mut id = self.next;
        while id == 0 || self.entries.contains_key(&id) {
            id = id.wrapping_add(1);
        }
        self.next = id.wrapping_add(1);
        if let Some(at) = deadline {
            let _fresh = self.deadlines.insert((at, id));
        }
        let _vacant = self.entries.insert(id, Slot { deadline, value });
        (id, evicted)
    }

    /// Removes request `id` and returns its value, or `None` if it is not
    /// outstanding.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(4);
    /// let (id, _) = p.issue(7, None);
    /// assert_eq!(p.settle(id), Some(7));
    /// assert_eq!(p.settle(id), None);
    /// ```
    pub fn settle(&mut self, id: u64) -> Option<T> {
        let slot = self.entries.remove(&id)?;
        self.forget_deadline(id, slot.deadline);
        Some(slot.value)
    }

    /// Removes and returns the request with the earliest deadline at or before
    /// `now`, if any. Call until `None` to expire everything due.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let mut p = rpc_io::Pending::new(4);
    /// let (late, _) = p.issue("late", Some(Duration::from_millis(20)));
    /// let (early, _) = p.issue("early", Some(Duration::from_millis(10)));
    /// let now = Duration::from_millis(30);
    /// assert_eq!(p.pop_expired(now), Some((early, "early")));
    /// assert_eq!(p.pop_expired(now), Some((late, "late")));
    /// assert_eq!(p.pop_expired(now), None);
    /// ```
    pub fn pop_expired(&mut self, now: Duration) -> Option<(u64, T)> {
        let &(at, id) = self.deadlines.first()?;
        if at > now {
            return None;
        }
        let _removed = self.deadlines.pop_first();
        self.entries.remove(&id).map(|slot| (id, slot.value))
    }

    /// The earliest deadline among outstanding requests.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// let mut p = rpc_io::Pending::new(4);
    /// assert_eq!(p.next_deadline(), None);
    /// p.issue((), Some(Duration::from_secs(3)));
    /// p.issue((), Some(Duration::from_secs(1)));
    /// assert_eq!(p.next_deadline(), Some(Duration::from_secs(1)));
    /// ```
    #[must_use]
    pub fn next_deadline(&self) -> Option<Duration> {
        self.deadlines.first().map(|&(at, _)| at)
    }

    /// Removes every outstanding request, oldest first.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut p = rpc_io::Pending::new(4);
    /// p.issue('a', None);
    /// p.issue('b', None);
    /// let all: Vec<_> = p.drain().map(|(_, v)| v).collect();
    /// assert_eq!(all, ['a', 'b']);
    /// assert!(p.is_empty());
    /// ```
    pub fn drain(&mut self) -> impl Iterator<Item = (u64, T)> + use<T> {
        self.deadlines.clear();
        core::mem::take(&mut self.entries)
            .into_iter()
            .map(|(id, slot)| (id, slot.value))
    }

    fn forget_deadline(&mut self, id: u64, deadline: Option<Duration>) {
        if let Some(at) = deadline {
            let _was_there = self.deadlines.remove(&(at, id));
        }
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use alloc::vec::Vec;

    use proptest::prelude::*;

    use super::*;

    /// The reference: a plain vector of `(id, deadline, value)`, oldest first.
    #[derive(Default)]
    struct Model {
        next: u64,
        entries: Vec<(u64, Option<Duration>, u32)>,
    }

    #[derive(Clone, Debug)]
    enum Op {
        Issue(u32, Option<u8>),
        Settle(u8),
        Expire(u8),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            (any::<u32>(), proptest::option::of(any::<u8>())).prop_map(|(v, d)| Op::Issue(v, d)),
            any::<u8>().prop_map(Op::Settle),
            any::<u8>().prop_map(Op::Expire),
        ]
    }

    proptest! {
        #[test]
        fn prop_matches_reference(capacity in 1usize..8, ops in proptest::collection::vec(op(), 0..200)) {
            let mut real = Pending::new(capacity);
            let mut model = Model { next: 1, ..Model::default() };
            for op in ops {
                match op {
                    Op::Issue(value, deadline) => {
                        let deadline = deadline.map(|d| Duration::from_millis(u64::from(d)));
                        let expected_evicted = (model.entries.len() >= capacity).then(|| {
                            let (id, _, v) = model.entries.remove(0);
                            (id, v)
                        });
                        let id = model.next;
                        model.next += 1;
                        model.entries.push((id, deadline, value));
                        prop_assert_eq!(real.issue(value, deadline), (id, expected_evicted));
                    }
                    Op::Settle(back) => {
                        let id = model.next.saturating_sub(u64::from(back % 10));
                        let expected = model.entries.iter().position(|e| e.0 == id).map(|i| model.entries.remove(i).2);
                        prop_assert_eq!(real.settle(id), expected);
                    }
                    Op::Expire(now) => {
                        let now = Duration::from_millis(u64::from(now));
                        // Earliest deadline first; ties by id.
                        let due = model
                            .entries
                            .iter()
                            .enumerate()
                            .filter_map(|(i, e)| e.1.filter(|&d| d <= now).map(|d| (d, e.0, i)))
                            .min();
                        let expected = due.map(|(_, _, i)| {
                            let e = model.entries.remove(i);
                            (e.0, e.2)
                        });
                        prop_assert_eq!(real.pop_expired(now), expected);
                    }
                }
                prop_assert_eq!(real.len(), model.entries.len());
                prop_assert!(real.len() <= capacity);
                let next_deadline = model.entries.iter().filter_map(|e| e.1).min();
                prop_assert_eq!(real.next_deadline(), next_deadline);
            }
        }
    }

    #[test]
    fn test_scale_settle_out_of_order_is_log_time() {
        // 200k requests settled in a scrambled order; a linear settle would
        // take on the order of 10^10 steps here.
        const N: u64 = 200_000;
        let mut p = Pending::new(N as usize);
        for i in 0..N {
            let _issued = p.issue(i, Some(Duration::from_nanos(i * 7 % 1000)));
        }
        let mut settled = 0u64;
        for k in 0..N {
            let id = (k * 7919) % N + 1; // 7919 is coprime with N: a permutation
            if p.settle(id).is_some() {
                settled += 1;
            }
        }
        assert_eq!(settled, N);
        assert!(p.is_empty());
        assert_eq!(p.next_deadline(), None);
    }

    #[test]
    fn test_scale_eviction_keeps_capacity() {
        let mut p = Pending::new(1000);
        let mut evicted = 0;
        for i in 0..150_000u32 {
            if p.issue(i, None).1.is_some() {
                evicted += 1;
            }
        }
        assert_eq!(p.len(), 1000);
        assert_eq!(evicted, 149_000);
    }
}
