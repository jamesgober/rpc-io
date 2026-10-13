//! Newline framing: any bytes, in two chunkings, through the decoder.
#![no_main]

#[path = "../../tests/harness/mod.rs"]
mod harness;

libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    harness::framing(rpc_io::Framing::Newline, data);
});
