//! Typed handlers at realistic scale: 100k LSP hover requests answered
//! through jsonx-lib's `FromJson`/`ToJson` (params borrowed from the message,
//! the result written into the endpoint's reused buffer), and, with the
//! `serde` feature, the same through `serde`. Plus 100k progress reports.

use std::{hint::black_box, time::Duration};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use rpc_io::jsonx::{DecodeError, FromJson, Reader, ToJson, ValueWriter, Written};
use rpc_io::{Endpoint, Framed, Framing, Progress, Raw, Router};

const MESSAGES: usize = 100_000;

fn request(id: usize) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"textDocument/hover","params":{{"textDocument":{{"uri":"file:///home/user/project/src/module_{m}.rs"}},"position":{{"line":{l},"character":17}}}}}}"#,
        m = id % 97,
        l = id % 4000,
    )
}

struct Doc<'a> {
    uri: &'a str,
}

impl<'de> FromJson<'de> for Doc<'de> {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let mut uri = None;
        let mut object = reader.read_object()?;
        while let Some(key) = object.next_key()? {
            if key == "uri" {
                object.field(&mut uri)?;
            }
        }
        Ok(Doc {
            uri: object.required(uri, "uri")?,
        })
    }
}

struct Pos {
    line: u64,
}

impl<'de> FromJson<'de> for Pos {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let (mut line, mut character) = (None, None::<u64>);
        let mut object = reader.read_object()?;
        while let Some(key) = object.next_key()? {
            match &*key {
                "line" => object.field(&mut line)?,
                "character" => object.field(&mut character)?,
                _ => {}
            }
        }
        let _character = object.required(character, "character")?;
        Ok(Pos {
            line: object.required(line, "line")?,
        })
    }
}

struct HoverParams<'a> {
    text_document: Doc<'a>,
    position: Pos,
}

impl<'de> FromJson<'de> for HoverParams<'de> {
    fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
        let (mut text_document, mut position) = (None, None);
        let mut object = reader.read_object()?;
        while let Some(key) = object.next_key()? {
            match &*key {
                "textDocument" => object.field(&mut text_document)?,
                "position" => object.field(&mut position)?,
                _ => {}
            }
        }
        Ok(HoverParams {
            text_document: object.required(text_document, "textDocument")?,
            position: object.required(position, "position")?,
        })
    }
}

struct Hover {
    value: u64,
}

impl ToJson for Hover {
    fn to_json(&self, out: ValueWriter<'_>) -> Written {
        out.object(|o| {
            o.member_with("contents", |c| {
                c.object(|c| {
                    c.member("kind", "markdown");
                    c.member("value", &self.value);
                })
            });
        })
    }
}

/// `FromJson` params borrowed from the message; a `ToJson` result written
/// into the endpoint's buffer.
fn json_router() -> Router<u64> {
    Router::new().request_encoded("textDocument/hover", |hits: &mut u64, params, _| {
        let p: HoverParams<'_> = params.decode()?;
        *hits += p.text_document.uri.len() as u64;
        Ok(Hover {
            value: p.position.line,
        })
    })
}

/// The owned-params path: `request_json`, the result written into the
/// endpoint's reused buffer.
fn request_json_router() -> Router<u64> {
    struct Owned {
        line: u64,
    }
    impl<'de> FromJson<'de> for Owned {
        fn from_json(reader: &mut Reader<'de>) -> Result<Self, DecodeError> {
            let p = HoverParams::from_json(reader)?;
            Ok(Owned {
                line: p.position.line + p.text_document.uri.len() as u64,
            })
        }
    }
    Router::new().request_json("textDocument/hover", |hits: &mut u64, p: Owned, _| {
        *hits += 1;
        Ok(Hover { value: p.line })
    })
}

#[cfg(feature = "serde")]
fn serde_router() -> Router<u64> {
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    struct Doc<'a> {
        #[serde(borrow)]
        uri: &'a str,
    }
    #[derive(Deserialize)]
    struct Pos {
        line: u64,
        #[allow(dead_code)]
        character: u64,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct HoverParams<'a> {
        #[serde(borrow)]
        text_document: Doc<'a>,
        position: Pos,
    }
    #[derive(Serialize)]
    struct Contents {
        kind: &'static str,
        value: u64,
    }
    #[derive(Serialize)]
    struct Hover {
        contents: Contents,
    }
    Router::new().request("textDocument/hover", |hits: &mut u64, params, _| {
        let p: HoverParams<'_> = params.deserialize()?;
        *hits += p.text_document.uri.len() as u64;
        rpc_io::Json::serialize(&Hover {
            contents: Contents {
                kind: "markdown",
                value: p.position.line,
            },
        })
    })
}

fn handle_all(
    c: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    name: &str,
    bodies: &[String],
    router: fn() -> Router<u64>,
) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                Endpoint::new(
                    Framed::new(std::io::sink(), Framing::ContentLength),
                    router(),
                    0,
                )
            },
            |mut endpoint| {
                for body in bodies {
                    let _flow = endpoint.handle(body.as_bytes());
                }
                black_box(*endpoint.state())
            },
            BatchSize::PerIteration,
        );
    });
}

fn bench_typed(c: &mut Criterion) {
    let bodies: Vec<String> = (0..MESSAGES).map(request).collect();
    let mut group = c.benchmark_group("typed");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(MESSAGES as u64));
    handle_all(
        &mut group,
        "handle_100k_json_borrowed",
        &bodies,
        json_router,
    );
    handle_all(
        &mut group,
        "handle_100k_request_json",
        &bodies,
        request_json_router,
    );
    #[cfg(feature = "serde")]
    handle_all(&mut group, "handle_100k_serde", &bodies, serde_router);
    group.bench_function("progress_100k_reports", |b| {
        b.iter_batched(
            || {
                let endpoint = Endpoint::new(
                    Framed::new(std::io::sink(), Framing::Newline),
                    Router::<()>::new(),
                    (),
                );
                let progress = Progress::new(endpoint.peer(), Raw::NULL);
                (endpoint, progress)
            },
            |(endpoint, progress)| {
                for step in 0..MESSAGES {
                    let _sent =
                        progress.report(step as f64, Some(MESSAGES as f64), Some("indexing"));
                }
                black_box(endpoint.stats().notifications_out)
            },
            BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_typed);
criterion_main!(benches);
