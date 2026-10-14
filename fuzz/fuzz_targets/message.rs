//! Message decoding: any body through `decode`, and every valid message
//! back through it.
#![no_main]

#[path = "../../tests/harness/mod.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    harness::message(data);
});
