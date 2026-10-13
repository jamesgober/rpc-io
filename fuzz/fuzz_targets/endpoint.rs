//! A live endpoint reading any bytes with either framing, over a channel or
//! a `Queued` writer with the timer thread: every request answered at least
//! once and no id more often than it was used. No panicking handlers:
//! libFuzzer aborts on any panic, contained or not.
#![no_main]

#[path = "../../tests/harness/mod.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    harness::endpoint(data, false);
});
