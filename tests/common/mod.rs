//! Helpers shared by the integration tests.
#![allow(dead_code)]

use std::{
    io::{self, Write},
    sync::{Arc, Mutex, PoisonError, mpsc},
};

use rpc_io::{Endpoint, Router, Sink};

/// A writer whose bytes can be read while the endpoint still owns it.
#[derive(Clone, Default)]
pub struct Shared(pub Arc<Mutex<Vec<u8>>>);

impl Shared {
    pub fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl Write for Shared {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// An endpoint over a channel sink, and the channel's receiving end.
pub fn endpoint<S>(
    router: Router<S>,
    state: S,
) -> (Endpoint<S, mpsc::Sender<String>>, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel();
    (Endpoint::new(tx, router, state), rx)
}

/// Everything sent so far.
pub fn sent(rx: &mpsc::Receiver<String>) -> Vec<String> {
    rx.try_iter().collect()
}

/// A sink that fails or panics on demand.
pub struct Faulty {
    pub panic: bool,
}

impl Sink for Faulty {
    fn send(&mut self, _message: &str) -> io::Result<()> {
        if self.panic {
            panic!("the sink panicked");
        }
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "gone"))
    }
}

/// The writing end of an in-memory byte pipe.
pub struct PipeWriter(mpsc::Sender<Vec<u8>>);

/// The reading end of an in-memory byte pipe: reads end when every writer
/// is gone.
pub struct PipeReader {
    rx: mpsc::Receiver<Vec<u8>>,
    chunk: Vec<u8>,
    at: usize,
}

/// A one-way in-memory byte stream.
pub fn pipe() -> (PipeWriter, PipeReader) {
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

impl Write for PipeWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        self.0
            .send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "reader gone"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl io::Read for PipeReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.at == self.chunk.len() {
            match self.rx.recv() {
                Ok(chunk) => {
                    self.chunk = chunk;
                    self.at = 0;
                }
                Err(_) => return Ok(0),
            }
        }
        let n = out.len().min(self.chunk.len() - self.at);
        out[..n].copy_from_slice(&self.chunk[self.at..self.at + n]);
        self.at += n;
        Ok(n)
    }
}

/// Silences panics whose message starts with "deliberate" (handlers that
/// panic on purpose); every other panic is reported as usual.
pub fn quiet_deliberate_panics() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            let payload = info.payload();
            let text = payload
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
            if !text.is_some_and(|t| t.starts_with("deliberate")) {
                default(info);
            }
        }));
    });
}
