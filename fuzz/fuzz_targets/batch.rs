//! Batch decoding: `[` + any bytes + `]` through `decode`.
#![no_main]

#[path = "../../tests/harness/mod.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    harness::batch(data);
});
