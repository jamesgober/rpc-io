//! The one-shot slot a request's outcome arrives in. The endpoint writes it
//! once (an answer, a timeout, a cancellation, the close); the request's
//! [`Call`](crate::Call) reads it by blocking, by polling, or as a future.

use std::{
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
    task::{Context, Poll, Waker},
    time::Instant,
};

use crate::{error::CallError, raw::Raw};

/// How one of our requests ended.
pub(crate) type Outcome = Result<Raw<'static>, CallError>;

/// A new slot: the writing end for the pending map, the reading end for the
/// `Call`.
pub(crate) fn slot() -> (Waiter, Arc<Slot>) {
    let slot = Arc::new(Slot {
        state: Mutex::new(State {
            outcome: None,
            filled: false,
            waker: None,
        }),
        ready: Condvar::new(),
    });
    (Waiter(Arc::clone(&slot)), slot)
}

pub(crate) struct Slot {
    state: Mutex<State>,
    ready: Condvar,
}

struct State {
    outcome: Option<Outcome>,
    /// Whether an outcome was written (it may have been taken since).
    filled: bool,
    /// The task polling the `Call`, if it is awaited.
    waker: Option<Waker>,
}

impl Slot {
    /// The state. Nothing that runs under this lock can panic, so the poison
    /// is ignored.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Writes the outcome, unless one was written before; wakes the reader.
    fn fill(&self, outcome: Outcome) {
        let waker = {
            let mut state = self.lock();
            if state.filled {
                return;
            }
            state.filled = true;
            state.outcome = Some(outcome);
            state.waker.take()
        };
        self.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Whether an outcome was written (and maybe taken since).
    pub(crate) fn is_filled(&self) -> bool {
        self.lock().filled
    }

    /// The outcome, if it was written: once, and `Closed` on later reads.
    pub(crate) fn try_take(&self) -> Option<Outcome> {
        let mut state = self.lock();
        take(&mut state)
    }

    /// Blocks until the outcome is written or `deadline` passes (`None` on a
    /// timeout; with no deadline, it waits until the outcome is written).
    pub(crate) fn wait_until(&self, deadline: Option<Instant>) -> Option<Outcome> {
        let mut state = self.lock();
        loop {
            if let Some(outcome) = take(&mut state) {
                return Some(outcome);
            }
            state = match deadline {
                None => self
                    .ready
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(at) => {
                    let left = at.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return None;
                    }
                    self.ready
                        .wait_timeout(state, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }

    /// The outcome if it was written; otherwise registers the task to wake.
    pub(crate) fn poll(&self, cx: &Context<'_>) -> Poll<Outcome> {
        let mut state = self.lock();
        if let Some(outcome) = take(&mut state) {
            return Poll::Ready(outcome);
        }
        if !state
            .waker
            .as_ref()
            .is_some_and(|waker| waker.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

fn take(state: &mut State) -> Option<Outcome> {
    match state.outcome.take() {
        Some(outcome) => Some(outcome),
        None if state.filled => Some(Err(CallError::Closed)),
        None => None,
    }
}

/// The writing end of a slot, kept in the pending map. Dropped without
/// writing (the map went away with the request still in it), it ends the
/// request as `Closed`, so no `Call` can wait forever.
pub(crate) struct Waiter(Arc<Slot>);

impl Waiter {
    /// Ends the request with `outcome`.
    pub(crate) fn send(self, outcome: Outcome) {
        self.0.fill(outcome);
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        // A no-op after `send`: the slot keeps the first outcome.
        self.0.fill(Err(CallError::Closed));
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use std::{
        task::Wake,
        thread,
        time::{Duration, Instant},
    };

    use super::*;

    struct Flag(std::sync::atomic::AtomicBool);

    impl Wake for Flag {
        fn wake(self: Arc<Self>) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn test_slot_keeps_the_first_outcome_and_then_reads_closed() {
        let (waiter, slot) = slot();
        assert!(slot.try_take().is_none());
        waiter.send(Err(CallError::TimedOut));
        assert_eq!(slot.try_take(), Some(Err(CallError::TimedOut)));
        assert_eq!(slot.try_take(), Some(Err(CallError::Closed)));
    }

    #[test]
    fn test_dropped_waiter_closes_the_slot() {
        let (waiter, slot) = slot();
        drop(waiter);
        assert_eq!(slot.wait_until(None), Some(Err(CallError::Closed)));
    }

    #[test]
    fn test_wait_until_times_out_then_sees_a_late_outcome() {
        let (waiter, slot) = slot();
        let soon = Instant::now() + Duration::from_millis(5);
        assert_eq!(slot.wait_until(Some(soon)), None);
        let writer = thread::spawn(move || waiter.send(Err(CallError::Cancelled)));
        assert_eq!(slot.wait_until(None), Some(Err(CallError::Cancelled)));
        assert!(writer.join().is_ok());
    }

    #[test]
    fn test_poll_registers_and_wakes_the_task() {
        let (waiter, slot) = slot();
        let flag = Arc::new(Flag(std::sync::atomic::AtomicBool::new(false)));
        let waker = Waker::from(Arc::clone(&flag));
        let cx = Context::from_waker(&waker);
        assert!(slot.poll(&cx).is_pending());
        assert!(slot.poll(&cx).is_pending()); // the same waker is kept
        waiter.send(Err(CallError::Evicted));
        assert!(flag.0.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(slot.poll(&cx), Poll::Ready(Err(CallError::Evicted)));
    }
}
