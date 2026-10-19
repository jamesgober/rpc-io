//! The interleaved CPU-cycle A/B behind the README's and the release notes'
//! cycle figures: rpc-io 0.3.0 against this tree, 100,000 distinct hover
//! requests per run on a fresh endpoint. See `Cargo.toml` for how to run it.

use std::time::Instant;

fn cycles() -> u64 {
    let mut c: u64 = 0;
    // SAFETY: valid pseudo-handle for the current thread and a valid out pointer.
    unsafe {
        windows_sys::Win32::System::WindowsProgramming::QueryThreadCycleTime(
            windows_sys::Win32::System::Threading::GetCurrentThread(),
            &mut c,
        );
    }
    c
}

const N: usize = 100_000;

fn request(id: usize) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"textDocument/hover","params":{{"textDocument":{{"uri":"file:///home/user/project/src/module_{m}.rs"}},"position":{{"line":{l},"character":17}}}}}}"#,
        m = id % 97,
        l = id % 4000,
    )
}

#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct Td {
    uri: String,
}
#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct Pos {
    line: u64,
    character: u64,
}
#[derive(serde::Deserialize)]
#[allow(dead_code)]
struct HoverP {
    #[serde(rename = "textDocument")]
    td: Td,
    position: Pos,
}
#[derive(serde::Serialize)]
struct Contents {
    kind: &'static str,
    value: u64,
}
#[derive(serde::Serialize)]
struct HoverR {
    contents: Contents,
}

macro_rules! suite {
    ($m:ident, $rpc:ident, $jx:path) => {
        mod $m {
            use $rpc::{Endpoint, Framed, Framing, Progress, Raw, Router};
            use $jx as jx;
            use super::*;
            pub fn unparsed(bodies: &[String]) -> (u64, f64) {
                let mut e = Endpoint::new(Framed::new(std::io::sink(), Framing::ContentLength),
                    Router::new().request("textDocument/hover", |_: &mut (), _, _| Ok(Raw::NULL)), ());
                let (c0, t0) = (cycles(), Instant::now());
                for b in bodies { let _ = e.handle(b.as_bytes()); }
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
            pub fn parsed(bodies: &[String]) -> (u64, f64) {
                let mut e = Endpoint::new(Framed::new(std::io::sink(), Framing::ContentLength),
                    Router::new().request("textDocument/hover", |hits: &mut u64, params, _| {
                        *hits += 1;
                        let p = params.parse()?;
                        let line = p["position"]["line"].as_u64().unwrap_or(0);
                        Ok(Raw::from_value(&jx::json!({ "contents": { "kind": "markdown", "value": line } })))
                    }), 0u64);
                let (c0, t0) = (cycles(), Instant::now());
                for b in bodies { let _ = e.handle(b.as_bytes()); }
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
            pub fn serde(bodies: &[String]) -> (u64, f64) {
                let mut e = Endpoint::new(Framed::new(std::io::sink(), Framing::ContentLength),
                    Router::new().request_typed("textDocument/hover", |_: &mut (), p: HoverP, _| {
                        Ok(HoverR { contents: Contents { kind: "markdown", value: p.position.line } })
                    }), ());
                let (c0, t0) = (cycles(), Instant::now());
                for b in bodies { let _ = e.handle(b.as_bytes()); }
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
            pub fn decode(bodies: &[String]) -> (u64, f64) {
                let limits = $rpc::Limits::new();
                let (c0, t0) = (cycles(), Instant::now());
                let mut n = 0usize;
                for b in bodies { if matches!($rpc::decode(b.as_bytes(), &limits), $rpc::Incoming::Single(Ok(_))) { n += 1; } }
                assert_eq!(n, bodies.len());
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
            pub fn run(wire: &[u8]) -> (u64, f64) {
                let mut e = Endpoint::new(Framed::new(std::io::sink(), Framing::ContentLength),
                    Router::new().request("textDocument/hover", |_: &mut (), _, _| Ok(Raw::NULL)), ());
                let (c0, t0) = (cycles(), Instant::now());
                let _ = e.run(wire, Framing::ContentLength);
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
            pub fn progress(n: usize) -> (u64, f64) {
                let e = Endpoint::new(Framed::new(std::io::sink(), Framing::Newline), Router::<()>::new(), ());
                let p = Progress::new(e.peer(), Raw::NULL);
                let (c0, t0) = (cycles(), Instant::now());
                for i in 0..n { let _ = p.report(i as f64 + 1.0, Some(1e9), Some("indexing")); }
                (cycles() - c0, t0.elapsed().as_secs_f64())
            }
        }
    };
}
suite!(v03, rpc03, jsonx02);
suite!(v04, rpc04, rpc04::jsonx);

fn stats(v: &mut Vec<f64>) -> (f64, f64) {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (v[0], v[v.len() / 2])
}

fn main() {
    let rounds: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(9);
    let bodies: Vec<String> = (0..N).map(request).collect();
    let mut wire = Vec::new();
    for b in &bodies {
        rpc04::Framing::ContentLength.encode(b.as_bytes(), &mut wire);
    }
    type W<'a> = (
        &'a str,
        Box<dyn Fn() -> (u64, f64) + 'a>,
        Box<dyn Fn() -> (u64, f64) + 'a>,
    );
    let works: Vec<W> = vec![
        (
            "endpoint, handler reads nothing",
            Box::new(|| v03::unparsed(&bodies)),
            Box::new(|| v04::unparsed(&bodies)),
        ),
        (
            "handler parses params",
            Box::new(|| v03::parsed(&bodies)),
            Box::new(|| v04::parsed(&bodies)),
        ),
        (
            "serde typed handler",
            Box::new(|| v03::serde(&bodies)),
            Box::new(|| v04::serde(&bodies)),
        ),
        (
            "decode alone",
            Box::new(|| v03::decode(&bodies)),
            Box::new(|| v04::decode(&bodies)),
        ),
        (
            "run, Content-Length stream",
            Box::new(|| v03::run(&wire)),
            Box::new(|| v04::run(&wire)),
        ),
        (
            "progress report",
            Box::new(|| v03::progress(N)),
            Box::new(|| v04::progress(N)),
        ),
    ];
    let only = std::env::args().nth(2).unwrap_or_default();
    for (name, a, b) in &works {
        if !name.contains(only.as_str()) {
            continue;
        }
        let (mut ca, mut cb, mut ta, mut tb) = (vec![], vec![], vec![], vec![]);
        for r in 0..rounds {
            let order: [bool; 2] = if r % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for second in order {
                let (c, t) = if second { b() } else { a() };
                if second {
                    cb.push(c as f64 / N as f64);
                    tb.push(N as f64 / t);
                } else {
                    ca.push(c as f64 / N as f64);
                    ta.push(N as f64 / t);
                }
            }
        }
        let (amin, amed) = stats(&mut ca);
        let (bmin, bmed) = stats(&mut cb);
        let (_, atm) = stats(&mut ta);
        let (_, btm) = stats(&mut tb);
        println!(
            "{name:34} cycles/req 0.3.0 min {amin:7.0} med {amed:7.0} | 0.4.0 min {bmin:7.0} med {bmed:7.0} | delta(min) {:+5.1}% delta(med) {:+5.1}% | wall med 0.3 {:.0} K/s 0.4 {:.0} K/s",
            (bmin / amin - 1.0) * 100.0,
            (bmed / amed - 1.0) * 100.0,
            atm / 1e3,
            btm / 1e3
        );
    }
}
