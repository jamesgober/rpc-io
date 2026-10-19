//! What the reused buffers promise, counted: once warm, a progress report
//! and a typed notification allocate nothing, and a `ToJson` result costs no
//! allocation beyond what any answered request costs.
//!
//! The counting allocator counts per thread, so tests running in parallel do
//! not see each other's allocations.
#![cfg(feature = "std")]

use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use rpc_io::jsonx::{ToJson, ValueWriter, Written};
use rpc_io::{Endpoint, Framed, Framing, Progress, Raw, Router};

struct Counting;

std::thread_local! {
    static ALLOCATIONS: Cell<u64> = const { Cell::new(0) };
}

fn count() {
    // `try_with`: the thread-local may already be gone while a thread exits.
    let _counted = ALLOCATIONS.try_with(|n| n.set(n.get() + 1));
}

// SAFETY: every method forwards to `System` with the arguments it was given,
// unchanged, so `System`'s guarantees hold; the counting touches only a
// `const`-initialised thread-local `Cell`, which does not allocate.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged; the caller upholds `alloc`'s contract.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged; `ptr` came from `System` through us.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged; `ptr` came from `System` through us.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Allocations per call of `f` on this thread, after warming it up.
fn per_call(mut f: impl FnMut(usize)) -> f64 {
    const CALLS: usize = 1000;
    for i in 0..CALLS {
        f(i);
    }
    let before = ALLOCATIONS.with(Cell::get);
    for i in 0..CALLS {
        f(i);
    }
    let after = ALLOCATIONS.with(Cell::get);
    (after - before) as f64 / CALLS as f64
}

struct Hover(u64);

impl ToJson for Hover {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.object(|o| {
            o.member_with("contents", |c| {
                c.object(|c| {
                    c.member("kind", "markdown");
                    c.member("value", &self.0);
                })
            });
        })
    }
}

#[test]
fn test_progress_reports_and_typed_notifications_allocate_nothing() {
    let endpoint = Endpoint::new(
        Framed::new(std::io::sink(), Framing::Newline),
        Router::<()>::new(),
        (),
    );
    let progress = Progress::new(endpoint.peer(), Raw::NULL);
    let mut step = 0.0;
    let reports = per_call(|_| {
        step += 1.5;
        assert!(progress.report(step, Some(1e9), Some("indexing")).is_ok());
    });
    assert_eq!(reports, 0.0);
    let peer = endpoint.peer();
    let notifications = per_call(|_| assert!(peer.notify_json("tick", &[1, 2, 3]).is_ok()));
    assert_eq!(notifications, 0.0);
}

#[test]
fn test_a_typed_result_costs_no_allocation_of_its_own() {
    let bodies: Vec<String> = (0..1000)
        .map(|id| format!(r#"{{"jsonrpc":"2.0","id":{id},"method":"m","params":[1]}}"#))
        .collect();
    let mut untyped = Endpoint::new(
        Framed::new(std::io::sink(), Framing::ContentLength),
        Router::new().request("m", |_: &mut (), _, _| Ok(Raw::NULL)),
        (),
    );
    let mut typed = Endpoint::new(
        Framed::new(std::io::sink(), Framing::ContentLength),
        Router::new().request_encoded("m", |_: &mut (), _, _| Ok(Hover(7))),
        (),
    );
    let baseline = per_call(|i| assert!(untyped.handle(bodies[i].as_bytes()).is_ok()));
    let encoded = per_call(|i| assert!(typed.handle(bodies[i].as_bytes()).is_ok()));
    assert_eq!(encoded, baseline, "a ToJson result allocated");
}
