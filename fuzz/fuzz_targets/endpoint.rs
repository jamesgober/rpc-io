//! A live endpoint reading any bytes with either framing. No panicking
//! handlers: libFuzzer aborts on any panic, contained or not.
#![no_main]

#[path = "../../tests/harness/mod.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    harness::endpoint(data, false);
});
