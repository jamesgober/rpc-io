//! The fuzz targets' checks (`tests/harness`), run on arbitrary bytes in
//! `cargo test`: proptest's random and JSON-shaped inputs, and a seeded
//! mutation loop over protocol-shaped seeds. `cargo fuzz` runs the same
//! checks coverage-guided (see `fuzz/`).
#![cfg(feature = "std")]

mod common;
mod harness;

use common::quiet_deliberate_panics;
use proptest::prelude::*;
use rpc_io::Framing;

/// Bytes that look like JSON-RPC: envelope pieces, framing, and noise.
fn shaped() -> impl Strategy<Value = Vec<u8>> {
    let piece = prop_oneof![
        Just(&b"{\"jsonrpc\":\"2.0\""[..]),
        Just(&b",\"id\":1"[..]),
        Just(&b",\"id\":\"a\""[..]),
        Just(&b",\"id\":null"[..]),
        Just(&b",\"method\":\"ok\""[..]),
        Just(&b",\"method\":\"later\""[..]),
        Just(&b",\"method\":\"note\""[..]),
        Just(&b",\"method\":\"ask\""[..]),
        Just(&b",\"method\":\"boom\""[..]),
        Just(&b",\"method\":\"$/cancelRequest\",\"params\":{\"id\":1}"[..]),
        Just(&b",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}"[..]),
        Just(&b",\"params\":[1,{\"a\":[]}]"[..]),
        Just(&b",\"result\":true"[..]),
        Just(&b",\"error\":{\"code\":1,\"message\":\"m\"}"[..]),
        Just(&b"}"[..]),
        Just(&b"["[..]),
        Just(&b"]"[..]),
        Just(&b","[..]),
        Just(&b"\n"[..]),
        Just(&b"\r\n"[..]),
        Just(&b"Content-Length: 40\r\n\r\n"[..]),
        Just(&b"Content-Length: 9999999999\r\n\r\n"[..]),
        Just(&b"Content-Type: x\r\n"[..]),
        Just(&b"\\u0000"[..]),
        Just(&b"\xff"[..]),
    ];
    (
        any::<u8>(),
        any::<u8>(),
        proptest::collection::vec(piece, 0..24),
    )
        .prop_map(|(knob, split, pieces)| {
            let mut out = vec![knob, split];
            for piece in pieces {
                out.extend_from_slice(piece);
            }
            out
        })
}

fn input() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![proptest::collection::vec(any::<u8>(), 0..300), shaped()]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn prop_content_length_framing(data in input()) {
        harness::framing(Framing::ContentLength, &data);
    }

    #[test]
    fn prop_newline_framing(data in input()) {
        harness::framing(Framing::Newline, &data);
    }

    #[test]
    fn prop_message_decoding(data in input()) {
        harness::message(&data);
    }

    #[test]
    fn prop_batch_decoding(data in input()) {
        harness::batch(&data);
    }

    #[test]
    fn prop_endpoint(data in input()) {
        quiet_deliberate_panics();
        harness::endpoint(&data, true);
    }
}

/// A small, seeded xorshift generator: the mutation loop is the same on
/// every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        // `n` is small: the remainder fits.
        (self.next() % n.max(1) as u64) as usize
    }
}

const SEEDS: &[&[u8]] = &[
    b"\x00\x00Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ok\"}",
    b"\x01\x05{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"later\"}\n{\"jsonrpc\":\"2.0\",\"method\":\"$/cancelRequest\",\"params\":{\"id\":1}}\n",
    b"\x0b\x02[{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ok\",\"params\":[1]},{\"jsonrpc\":\"2.0\",\"method\":\"note\"},7]\n",
    b"\x03\x09{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":{\"a\":[1,2]}}\n{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32700,\"message\":\"x\"}}\n",
    b"\x86\x11Content-Length: 3\r\ncontent-type: a\r\n\r\n[1]\r\nContent-Length: 2\r\n\r\n{}",
    b"\x01\x03{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ask\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"later\"}\n{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ok\"}\n",
    b"\x00\x07Content-Length: 39\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ask\"}",
];

/// Bytes worth inserting: JSON punctuation, line ends, digits, letters.
const ALPHABET: &[u8] = b"{}[]\":,\n\r0123456789abc\\";

/// Applies one random edit: flip a byte, insert, delete, or splice in a
/// piece of another seed.
fn mutate(rng: &mut Rng, data: &mut Vec<u8>) {
    let at = rng.below(data.len() + 1);
    match rng.below(4) {
        0 if !data.is_empty() => {
            let i = at.min(data.len() - 1);
            data[i] ^= 1 << rng.below(8);
        }
        1 => data.insert(at, ALPHABET[rng.below(ALPHABET.len())]),
        2 if !data.is_empty() => {
            let _removed = data.remove(at.min(data.len() - 1));
        }
        _ => {
            let seed = SEEDS[rng.below(SEEDS.len())];
            let from = rng.below(seed.len());
            let len = rng.below(seed.len() - from + 1);
            let _spliced = data.splice(at..at, seed[from..from + len].iter().copied());
        }
    }
}

#[test]
fn test_seeded_mutations_hold_every_invariant() {
    quiet_deliberate_panics();
    let mut rng = Rng(0x5eed_1234_abcd_0001);
    for round in 0..20_000 {
        let mut data = SEEDS[round % SEEDS.len()].to_vec();
        for _ in 0..1 + rng.below(8) {
            mutate(&mut rng, &mut data);
        }
        harness::framing(Framing::ContentLength, &data);
        harness::framing(Framing::Newline, &data);
        harness::message(&data[1.min(data.len())..]);
        harness::batch(&data);
        harness::endpoint(&data, round % 2 == 0);
    }
}

#[test]
fn test_harness_on_empty_and_tiny_inputs() {
    for data in [&b""[..], b"\x00", b"\x00\x00", b"\xff\xff\xff"] {
        harness::framing(Framing::ContentLength, data);
        harness::framing(Framing::Newline, data);
        harness::message(data);
        harness::batch(data);
        harness::endpoint(data, false);
    }
}
