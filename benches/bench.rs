//! Criterion benchmarks at realistic scale: 100k messages through each layer
//! (framing, envelope decoding, the endpoint end to end), full batches, and
//! the correlation map.

use std::{hint::black_box, time::Duration};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rpc_io::{Endpoint, Framed, Framing, Incoming, Limits, Pending, Queued, Raw, Router, decode};

const MESSAGES: usize = 100_000;

/// An LSP-shaped request: a nested object of the size editors send for hover
/// and completion.
fn request(id: usize) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"textDocument/hover","params":{{"textDocument":{{"uri":"file:///home/user/project/src/module_{m}.rs"}},"position":{{"line":{l},"character":17}}}}}}"#,
        m = id % 97,
        l = id % 4000,
    )
}

fn framed(framing: Framing, bodies: &[String]) -> Vec<u8> {
    let mut wire = Vec::new();
    for body in bodies {
        framing.encode(body.as_bytes(), &mut wire);
    }
    wire
}

fn bench_framing(c: &mut Criterion) {
    let bodies: Vec<String> = (0..MESSAGES).map(request).collect();
    let mut group = c.benchmark_group("framing");
    group.sample_size(20);
    for framing in [Framing::ContentLength, Framing::Newline] {
        let wire = framed(framing, &bodies);
        group.throughput(Throughput::Bytes(wire.len() as u64));
        group.bench_function(format!("decode_100k_{framing:?}"), |b| {
            b.iter(|| {
                let mut decoder = framing.decoder(&Limits::new());
                let mut count = 0usize;
                let mut rest = wire.as_slice();
                while !rest.is_empty() {
                    let n = decoder.feed(rest);
                    rest = &rest[n..];
                    while let Ok(Some(frame)) = decoder.next_frame() {
                        count += frame.len();
                    }
                }
                black_box(count)
            });
        });
        group.bench_function(format!("encode_100k_{framing:?}"), |b| {
            let mut out = Vec::with_capacity(wire.len());
            b.iter(|| {
                out.clear();
                for body in &bodies {
                    framing.encode(body.as_bytes(), &mut out);
                }
                black_box(out.len())
            });
        });
    }
    group.finish();
}

fn bench_messages(c: &mut Criterion) {
    let bodies: Vec<String> = (0..MESSAGES).map(request).collect();
    let limits = Limits::new();
    let mut group = c.benchmark_group("messages");
    group.sample_size(20);
    group.throughput(Throughput::Elements(MESSAGES as u64));
    group.bench_function("decode_100k_requests", |b| {
        b.iter(|| {
            let mut ok = 0usize;
            for body in &bodies {
                if matches!(decode(body.as_bytes(), &limits), Incoming::Single(Ok(_))) {
                    ok += 1;
                }
            }
            black_box(ok)
        });
    });
    let batch = format!("[{}]", bodies[..1024].join(","));
    group.throughput(Throughput::Elements(1024));
    group.bench_function("decode_batch_1024", |b| {
        b.iter(|| black_box(decode(batch.as_bytes(), &limits)));
    });
    let responses: Vec<String> = (0..MESSAGES)
        .map(|i| format!(r#"{{"jsonrpc":"2.0","id":{i},"result":{{"contents":{{"kind":"markdown","value":"fn f() -> u32"}}}}}}"#))
        .collect();
    group.throughput(Throughput::Elements(MESSAGES as u64));
    group.bench_function("decode_and_write_100k_responses", |b| {
        let mut out = String::new();
        b.iter(|| {
            out.clear();
            for body in &responses {
                if let Incoming::Single(Ok(message)) = decode(body.as_bytes(), &limits) {
                    message.write(&mut out);
                }
            }
            black_box(out.len())
        });
    });
    group.finish();
}

fn hover_router() -> Router<u64> {
    Router::new().request("textDocument/hover", |hits: &mut u64, params, _| {
        *hits += 1;
        let p = params.parse()?;
        let line = p["position"]["line"].as_u64().unwrap_or(0);
        Ok(Raw::from_value(
            &jsonx_lib::json!({ "contents": { "kind": "markdown", "value": line } }),
        ))
    })
}

fn bench_endpoint(c: &mut Criterion) {
    let bodies: Vec<String> = (0..MESSAGES).map(request).collect();
    let wire = framed(Framing::ContentLength, &bodies);
    let mut group = c.benchmark_group("endpoint");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(MESSAGES as u64));
    group.bench_function("handle_100k_requests", |b| {
        b.iter_batched(
            || {
                Endpoint::new(
                    Framed::new(std::io::sink(), Framing::ContentLength),
                    hover_router(),
                    0,
                )
            },
            |mut endpoint| {
                for body in &bodies {
                    let _flow = endpoint.handle(body.as_bytes());
                }
                black_box(*endpoint.state())
            },
            BatchSize::PerIteration,
        );
    });
    // The endpoint's own cost: a handler that reads nothing and answers
    // `null`, so neither params parsing nor result building is measured.
    group.bench_function("handle_100k_unparsed", |b| {
        b.iter_batched(
            || {
                let router = Router::new().request("textDocument/hover", |hits: &mut u64, _, _| {
                    *hits += 1;
                    Ok(Raw::NULL)
                });
                Endpoint::new(
                    Framed::new(std::io::sink(), Framing::ContentLength),
                    router,
                    0,
                )
            },
            |mut endpoint| {
                for body in &bodies {
                    let _flow = endpoint.handle(body.as_bytes());
                }
                black_box(*endpoint.state())
            },
            BatchSize::PerIteration,
        );
    });
    group.bench_function("run_100k_framed_requests", |b| {
        b.iter_batched(
            || {
                Endpoint::new(
                    Framed::new(std::io::sink(), Framing::ContentLength),
                    hover_router(),
                    0,
                )
            },
            |mut endpoint| {
                let _end = endpoint.run(wire.as_slice(), Framing::ContentLength);
                black_box(*endpoint.state())
            },
            BatchSize::PerIteration,
        );
    });
    // Through the non-blocking output path: the writer thread does the
    // writes, and dropping the endpoint waits for it to finish them.
    group.bench_function("run_100k_queued", |b| {
        b.iter_batched(
            || {
                let sink = Queued::new(std::io::sink(), Framing::ContentLength);
                sink.map(|sink| Endpoint::new(sink, hover_router(), 0))
            },
            |endpoint| {
                let Ok(mut endpoint) = endpoint else {
                    return black_box(0);
                };
                let _end = endpoint.run(wire.as_slice(), Framing::ContentLength);
                black_box(*endpoint.state())
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

fn bench_pending(c: &mut Criterion) {
    let mut group = c.benchmark_group("pending");
    group.sample_size(20);
    group.throughput(Throughput::Elements(MESSAGES as u64));
    group.bench_function("issue_then_settle_100k_out_of_order", |b| {
        b.iter(|| {
            let mut pending = Pending::new(MESSAGES);
            for i in 0..MESSAGES {
                let _issued = pending.issue(i, Some(Duration::from_millis(i as u64 % 1000)));
            }
            let mut settled = 0usize;
            for k in 0..MESSAGES as u64 {
                // 7919 is coprime with 100 000: a permutation of the ids.
                if pending.settle(k * 7919 % MESSAGES as u64 + 1).is_some() {
                    settled += 1;
                }
            }
            black_box(settled)
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_framing,
    bench_messages,
    bench_endpoint,
    bench_pending
);
criterion_main!(benches);
