//! A client and a server talking over an in-memory pipe, each on its own
//! thread: requests from several threads correlated by id, a deferred answer
//! from a worker, a notification, and a cancelled request.
//!
//! ```text
//! $ cargo run --example pipe_pair
//! ```

use std::{
    io::{self, Read, Write},
    sync::mpsc,
    thread,
    time::Duration,
};

use jsonx_lib::json;
use rpc_io::{CallError, End, Endpoint, ErrorObject, Framed, Framing, Router};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (to_server, server_input) = pipe();
    let (to_client, client_input) = pipe();

    // The server: one plain method, one answered later by a worker thread
    // (which watches for cancellation), and `exit`.
    let server = thread::spawn(move || {
        let router = Router::new()
            .request("add", |_: &mut (), params, _| {
                let p = params.parse()?;
                match (p[0].as_i64(), p[1].as_i64()) {
                    (Some(a), Some(b)) => Ok(json!(a + b)),
                    _ => Err(ErrorObject::invalid_params("expected [a, b]")),
                }
            })
            .deferred("slow", |_, params, cx, responder| {
                let millis = params
                    .parse()
                    .ok()
                    .and_then(|p| p["millis"].as_u64())
                    .unwrap_or(0);
                let peer = cx.peer();
                thread::spawn(move || {
                    for step in 0..millis / 10 {
                        if responder.is_cancelled() {
                            return; // the endpoint has answered for us
                        }
                        peer.notify("progress", json!({ "step": step })).ok();
                        thread::sleep(Duration::from_millis(10));
                    }
                    responder.ok("finished").ok();
                });
            })
            .notification("exit", |_, _, cx| cx.stop());
        rpc_io::serve(server_input, to_client, Framing::ContentLength, router)
    });

    // The client: an endpoint whose router only counts progress
    // notifications; its reader runs on a thread, requests go out from here.
    let router = Router::new().notification("progress", |seen: &mut u32, _, _| *seen += 1);
    let mut client = Endpoint::new(Framed::new(to_server, Framing::ContentLength), router, 0u32);
    let peer = client.peer();
    let reader = thread::spawn(move || {
        let end = client.run(client_input, Framing::ContentLength);
        (end, *client.state())
    });

    let adders: Vec<_> = (0..4)
        .map(|t| {
            let peer = peer.clone();
            thread::spawn(move || -> Result<i64, CallError> {
                let mut total = 0;
                for i in 0..100 {
                    let call = peer
                        .request("add", json!([t, i]))
                        .map_err(|_| CallError::Closed)?;
                    let result = call.wait()?;
                    total += result.parse().ok().and_then(|v| v.as_i64()).unwrap_or(0);
                }
                Ok(total)
            })
        })
        .collect();
    for adder in adders {
        let total = adder.join().map_err(|_| "an adder panicked")??;
        println!("adder total: {total}");
    }

    let quick = peer.request("slow", json!({ "millis": 30 }))?;
    println!("slow call: {:?}", quick.wait()?.as_str());

    let doomed = peer.request("slow", json!({ "millis": 5000 }))?;
    let canceller = doomed.canceller();
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        canceller.cancel();
    });
    println!("cancelled call: {:?}", doomed.wait());

    peer.notify("exit", ())?;
    let server_end = server.join().map_err(|_| "the server panicked")??;
    let (client_end, progress) = reader.join().map_err(|_| "the client panicked")?;
    println!(
        "server: {server_end:?}, client: {:?}, progress notifications: {progress}",
        client_end?
    );
    assert_eq!(server_end, End::Stopped);
    Ok(())
}

/// A one-way in-memory byte stream: the writing end and the reading end.
fn pipe() -> (PipeWriter, PipeReader) {
    let (tx, rx) = mpsc::channel();
    (
        PipeWriter(tx),
        PipeReader {
            rx,
            chunk: Vec::new(),
            at: 0,
        },
    )
}

struct PipeWriter(mpsc::Sender<Vec<u8>>);

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "the reading end is gone"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct PipeReader {
    rx: mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    at: usize,
}

impl Read for PipeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.at == self.chunk.len() {
            match self.rx.recv() {
                Ok(chunk) => (self.chunk, self.at) = (chunk, 0),
                Err(_) => return Ok(0), // every writer is gone: end of stream
            }
        }
        let n = out.len().min(self.chunk.len() - self.at);
        out[..n].copy_from_slice(&self.chunk[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}
