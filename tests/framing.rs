//! The framings against hostile and malformed byte streams.

use rpc_io::{FrameError, Framing, Limits, READ_CHUNK, decode};

/// Every frame (or error) the decoder produces from `input` fed `chunk`
/// bytes at a time, then ended.
fn frames(
    framing: Framing,
    limits: &Limits,
    input: &[u8],
    chunk: usize,
) -> Vec<Result<Vec<u8>, FrameError>> {
    let mut decoder = framing.decoder(limits);
    let mut out = Vec::new();
    let mut rest = input;
    loop {
        loop {
            match decoder.next_frame() {
                Ok(Some(frame)) => out.push(Ok(frame.to_vec())),
                Ok(None) => break,
                Err(error) => {
                    out.push(Err(error));
                    if !error.is_recoverable() {
                        return out;
                    }
                }
            }
        }
        if rest.is_empty() {
            break;
        }
        let n = decoder.feed(&rest[..rest.len().min(chunk)]);
        rest = &rest[n..];
    }
    loop {
        match decoder.finish() {
            Ok(Some(frame)) => out.push(Ok(frame.to_vec())),
            Ok(None) => return out,
            Err(error) => {
                out.push(Err(error));
                if !error.is_recoverable() {
                    return out;
                }
            }
        }
    }
}

fn content_length(input: &[u8]) -> Vec<Result<Vec<u8>, FrameError>> {
    frames(Framing::ContentLength, &Limits::new(), input, 5)
}

fn ok(bodies: &[&[u8]]) -> Vec<Result<Vec<u8>, FrameError>> {
    bodies.iter().map(|b| Ok(b.to_vec())).collect()
}

#[test]
fn test_content_length_overflowing_u64_is_invalid() {
    for digits in ["18446744073709551616", "99999999999999999999999999999999"] {
        let input = format!("Content-Length: {digits}\r\n\r\n{{}}");
        assert_eq!(
            content_length(input.as_bytes()),
            [Err(FrameError::InvalidContentLength)],
            "{digits}"
        );
    }
}

#[test]
fn test_content_length_beyond_the_limit_is_skipped_and_the_stream_recovers() {
    let limits = Limits::new().with_max_message_bytes(8);
    let input = b"Content-Length: 9\r\n\r\n123456789Content-Length: 2\r\n\r\n{}";
    assert_eq!(
        frames(Framing::ContentLength, &limits, input, 3),
        [
            Err(FrameError::TooLarge {
                length: Some(9),
                limit: 8
            }),
            Ok(b"{}".to_vec())
        ]
    );
    // Exactly at the limit is accepted.
    let input = b"Content-Length: 8\r\n\r\n12345678";
    assert_eq!(
        frames(Framing::ContentLength, &limits, input, 3),
        ok(&[b"12345678"])
    );
}

#[test]
fn test_content_length_u64_max_is_skipped_until_the_stream_ends() {
    let got = content_length(b"Content-Length: 18446744073709551615\r\n\r\nabc");
    assert_eq!(
        got,
        [
            Err(FrameError::TooLarge {
                length: Some(u64::MAX),
                limit: Limits::new().max_message_bytes
            }),
            Err(FrameError::Truncated),
        ]
    );
}

#[test]
fn test_duplicate_content_length_is_refused_even_when_equal() {
    for input in [
        &b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}"[..],
        b"Content-Length: 2\r\ncontent-length: 3\r\n\r\n{}x",
    ] {
        assert_eq!(
            content_length(input),
            [Err(FrameError::DuplicateContentLength)]
        );
    }
}

#[test]
fn test_malformed_content_length_values() {
    for value in ["", "-1", "+1", "0x10", "1 2", "1.0", "１"] {
        let input = format!("Content-Length: {value}\r\n\r\n");
        assert_eq!(
            content_length(input.as_bytes()),
            [Err(FrameError::InvalidContentLength)],
            "{value:?}"
        );
    }
    // Leading zeros and surrounding blanks are fine.
    assert_eq!(
        content_length(b"Content-Length: \t002 \r\n\r\n{}"),
        ok(&[b"{}"])
    );
}

#[test]
fn test_missing_blank_line_between_header_and_body() {
    // The body is read as a header line: it is not `name: value`.
    assert_eq!(
        content_length(b"Content-Length: 2\r\n{}\r\n"),
        [Err(FrameError::InvalidHeader)]
    );
    // At the end of the stream it is a truncated header section.
    assert_eq!(
        content_length(b"Content-Length: 2\r\n{}"),
        [Err(FrameError::Truncated)]
    );
}

#[test]
fn test_extra_line_ends_between_messages_are_skipped() {
    let input = b"\r\n\r\nContent-Length: 2\r\n\r\n{}\r\n\nContent-Length: 2\n\n[]\r\n";
    assert_eq!(content_length(input), ok(&[b"{}", b"[]"]));
}

#[test]
fn test_extra_line_end_after_the_headers_is_body() {
    // One `\r\n` too many: it is the first two body bytes, as the length
    // says; the stream then loses its boundary.
    let got = content_length(b"Content-Length: 2\r\n\r\n\r\n{}\r\n");
    assert_eq!(got, [Ok(b"\r\n".to_vec()), Err(FrameError::InvalidHeader)]);
}

#[test]
fn test_header_names_are_case_insensitive_and_others_are_ignored() {
    let input =
        b"content-TYPE: application/vscode-jsonrpc; charset=utf-8\r\nCONTENT-LENGTH: 4\r\n\r\nnull";
    assert_eq!(content_length(input), ok(&[b"null"]));
}

#[test]
fn test_header_section_budgets() {
    let limits = Limits::new().with_max_header_bytes(32).with_max_headers(2);
    let many = b"A: 1\r\nB: 2\r\nC: 3\r\n\r\n";
    assert_eq!(
        frames(Framing::ContentLength, &limits, many, 4),
        [Err(FrameError::TooManyHeaders { limit: 2 })]
    );
    let long = format!("X: {}\r\nContent-Length: 2\r\n\r\n{{}}", "y".repeat(40));
    assert_eq!(
        frames(Framing::ContentLength, &limits, long.as_bytes(), 4),
        [Err(FrameError::HeaderTooLarge { limit: 32 })]
    );
    // A header line that never ends is refused at the budget, not buffered.
    let endless = vec![b'h'; 10_000];
    assert_eq!(
        frames(Framing::ContentLength, &limits, &endless, 1000),
        [Err(FrameError::HeaderTooLarge { limit: 32 })]
    );
}

#[test]
fn test_missing_content_length_and_bad_lines() {
    assert_eq!(
        content_length(b"Content-Type: x\r\n\r\n{}"),
        [Err(FrameError::MissingContentLength)]
    );
    assert_eq!(
        content_length(b"no colon here\r\n\r\n"),
        [Err(FrameError::InvalidHeader)]
    );
    assert_eq!(
        content_length(b": empty name\r\n\r\n"),
        [Err(FrameError::InvalidHeader)]
    );
}

#[test]
fn test_fatal_errors_repeat() {
    let mut decoder = Framing::ContentLength.decoder(&Limits::new());
    let _taken = decoder.feed(b"bad\r\n");
    assert_eq!(decoder.next_frame(), Err(FrameError::InvalidHeader));
    assert_eq!(decoder.next_frame(), Err(FrameError::InvalidHeader));
    assert!(decoder.space().is_empty());
    assert_eq!(decoder.feed(b"Content-Length: 2\r\n\r\n{}"), 0);
}

#[test]
fn test_zero_length_body_is_an_empty_frame() {
    assert_eq!(content_length(b"Content-Length: 0\r\n\r\n"), ok(&[b""]));
    // ...which is not a message: JSON-RPC calls it a parse error.
    let rpc_io::Incoming::Single(Err(invalid)) = decode(b"", &Limits::new()) else {
        panic!("an empty body decoded as a message");
    };
    assert_eq!(invalid.error().code(), rpc_io::ErrorCode::PARSE_ERROR);
}

#[test]
fn test_body_delivered_one_byte_at_a_time_stays_within_the_memory_bound() {
    let limits = Limits::new().with_max_message_bytes(100_000);
    let mut input = b"Content-Length: 100000\r\n\r\n".to_vec();
    input.extend(std::iter::repeat_n(b'1', 100_000));
    let mut decoder = Framing::ContentLength.decoder(&limits);
    let mut seen = 0;
    for &byte in &input {
        assert_eq!(decoder.feed(&[byte]), 1);
        while let Ok(Some(frame)) = decoder.next_frame() {
            seen += frame.len();
        }
        assert!(decoder.buffered() <= decoder.memory_bound());
    }
    assert_eq!(seen, 100_000);
}

#[test]
fn test_newline_framing_with_embedded_newlines_splits_into_lines() {
    // A pretty-printed message is several lines, each its own (broken)
    // message: newline framing cannot carry raw line breaks. MCP forbids them.
    let input = b"{\n  \"jsonrpc\": \"2.0\",\n  \"method\": \"x\"\n}\n";
    let got = frames(Framing::Newline, &Limits::new(), input, 7);
    assert_eq!(got.len(), 4);
    // The encoder never produces them: line breaks become spaces, which JSON
    // reads as the same whitespace.
    let mut framed = Vec::new();
    Framing::Newline.encode(
        b"{\n  \"jsonrpc\": \"2.0\",\r\n  \"method\": \"x\"\n}",
        &mut framed,
    );
    let got = frames(Framing::Newline, &Limits::new(), &framed, 7);
    assert_eq!(got.len(), 1);
    let Some(Ok(body)) = got.first() else {
        panic!("no frame")
    };
    assert!(matches!(
        decode(body, &Limits::new()),
        rpc_io::Incoming::Single(Ok(_))
    ));
}

#[test]
fn test_newline_framing_line_ends_and_blank_lines() {
    let input = b"\r\n \t \n[1]\r\n\n\n{}\n2";
    assert_eq!(
        frames(Framing::Newline, &Limits::new(), input, 3),
        ok(&[b"[1]", b"{}", b"2"])
    );
}

#[test]
fn test_newline_framing_oversized_line_is_skipped_and_recovers() {
    let limits = Limits::new().with_max_message_bytes(4);
    let mut input = vec![b'z'; 3 * READ_CHUNK];
    input.extend_from_slice(b"\n[1]\n");
    let got = frames(Framing::Newline, &limits, &input, 999);
    assert_eq!(
        got,
        [
            Err(FrameError::TooLarge {
                length: None,
                limit: 4
            }),
            Ok(b"[1]".to_vec())
        ]
    );
    // A final oversized line without a line end is reported too.
    let got = frames(Framing::Newline, &limits, b"12345", 2);
    assert_eq!(
        got,
        [Err(FrameError::TooLarge {
            length: None,
            limit: 4
        })]
    );
    // One that outgrows the limit before the stream ends is cut off early.
    let got = frames(Framing::Newline, &limits, b"123456", 2);
    assert_eq!(
        got,
        [Err(FrameError::TooLarge {
            length: None,
            limit: 4
        })]
    );
}

#[test]
fn test_newline_framing_a_cr_before_the_line_end_does_not_count_against_the_limit() {
    let limits = Limits::new().with_max_message_bytes(4);
    assert_eq!(
        frames(Framing::Newline, &limits, b"1234\r\n", 1),
        ok(&[b"1234"])
    );
}

#[test]
fn test_content_length_encode_decode_any_body() {
    let bodies: [&[u8]; 4] = [
        b"",
        b"{}",
        b"\r\n\r\nContent-Length: 9\r\n\r\n",
        &[0xff, 0, b'\n'],
    ];
    let mut wire = Vec::new();
    for body in bodies {
        Framing::ContentLength.encode(body, &mut wire);
    }
    assert_eq!(content_length(&wire), ok(&bodies));
}
