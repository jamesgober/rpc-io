//! The async adapter: `Call` as a future, `Endpoint::run_async`, and the
//! `Outbox` drained by an async writer. A minimal executor stands in for a
//! runtime; nothing here depends on one.
#![cfg(feature = "futures-io")]

mod common;

use std::{
    future::Future,
    io,
    pin::{Pin, pin},
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread::{self, Thread},
    time::Duration,
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};
use jsonx_lib::json;
use rpc_io::{CallError, End, Endpoint, Error, Framing, QueueLimits, Router};

/// Runs `future` to completion on this thread.
fn block_on<F: Future>(future: F) -> F::Output {
    struct Unpark(Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
            return output;
        }
        thread::park();
    }
}

/// An async reader fed from a channel, a chunk at a time; `Pending` until a
/// chunk arrives, ended when the sender is dropped.
struct ChannelReader {
    chunks: mpsc::Receiver<Vec<u8>>,
    waiting: mpsc::Sender<Waker>,
    chunk: Vec<u8>,
    at: usize,
}

impl AsyncRead for ChannelReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        while this.at == this.chunk.len() {
            match this.chunks.try_recv() {
                Ok(chunk) => (this.chunk, this.at) = (chunk, 0),
                Err(mpsc::TryRecvError::Disconnected) => return Poll::Ready(Ok(0)),
                Err(mpsc::TryRecvError::Empty) => {
                    let _registered = this.waiting.send(cx.waker().clone());
                    // A chunk may have arrived before the waker was handed over.
                    match this.chunks.try_recv() {
                        Ok(chunk) => (this.chunk, this.at) = (chunk, 0),
                        Err(mpsc::TryRecvError::Disconnected) => return Poll::Ready(Ok(0)),
                        Err(mpsc::TryRecvError::Empty) => return Poll::Pending,
                    }
                }
            }
        }
        let n = buf.len().min(this.chunk.len() - this.at);
        buf[..n].copy_from_slice(&this.chunk[this.at..this.at + n]);
        this.at += n;
        Poll::Ready(Ok(n))
    }
}

/// A writer that accepts at most 5 bytes per write and is `Pending` every
/// other call, like a socket with a small buffer.
#[derive(Default)]
struct Trickle {
    out: Vec<u8>,
    flushes: usize,
    turn: bool,
}

impl AsyncWrite for Trickle {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        this.turn = !this.turn;
        if this.turn {
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let n = buf.len().min(5);
        this.out.extend_from_slice(&buf[..n]);
        Poll::Ready(Ok(n))
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().flushes += 1;
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

fn echo() -> Router<()> {
    Router::new()
        .request("echo", |_, params, _| {
            Ok(params
                .into_raw()
                .map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
        })
        .notification("exit", |_, _, cx| cx.stop())
}

#[test]
fn test_run_async_serves_a_stream_that_arrives_in_pieces() {
    let (chunks, received) = mpsc::channel::<Vec<u8>>();
    let (waiting, wakers) = mpsc::channel::<Waker>();
    let feeder = thread::spawn(move || {
        let mut wire = Vec::new();
        for i in 0..20 {
            let body = format!(r#"{{"jsonrpc":"2.0","id":{i},"method":"echo","params":[{i}]}}"#);
            Framing::ContentLength.encode(body.as_bytes(), &mut wire);
        }
        for piece in wire.chunks(17) {
            let _sent = chunks.send(piece.to_vec());
            while let Ok(waker) = wakers.try_recv() {
                waker.wake();
            }
            thread::sleep(Duration::from_micros(200));
        }
        drop(chunks);
        while let Ok(waker) = wakers.recv_timeout(Duration::from_millis(200)) {
            waker.wake();
        }
    });
    let input = ChannelReader {
        chunks: received,
        waiting,
        chunk: Vec::new(),
        at: 0,
    };
    let (sink, mut outbox) = rpc_io::outbox(Framing::ContentLength, QueueLimits::new());
    let mut endpoint = Endpoint::new(sink, echo(), ());
    let end = block_on(endpoint.run_async(input, Framing::ContentLength));
    assert!(matches!(end, Ok(End::Eof)), "{end:?}");
    assert!(feeder.join().is_ok());
    let mut writer = Trickle::default();
    assert!(block_on(outbox.write_to(&mut writer)).is_ok());
    let text = String::from_utf8_lossy(&writer.out).into_owned();
    assert_eq!(text.matches("Content-Length").count(), 20, "{text}");
    assert!(
        text.ends_with(r#"{"jsonrpc":"2.0","id":19,"result":[19]}"#),
        "{text}"
    );
    assert!(writer.flushes >= 2);
}

#[test]
fn test_run_async_stops_and_resumes() {
    let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"echo\"}\n";
    let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
    let mut endpoint = Endpoint::new(sink, echo(), ());
    let mut reader = input;
    assert!(matches!(
        block_on(endpoint.run_async(&mut reader, Framing::Newline)),
        Ok(End::Stopped)
    ));
    assert!(matches!(
        block_on(endpoint.run_async(&mut reader, Framing::Newline)),
        Ok(End::Eof)
    ));
    let mut out = Vec::new();
    assert!(block_on(outbox.write_to(&mut out)).is_ok());
    assert_eq!(out, b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":null}\n");
}

#[test]
fn test_run_async_reports_read_errors_and_closes() {
    struct Failing(bool);
    impl AsyncRead for Failing {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut [u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.0 {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::ConnectionReset, "reset")));
            }
            this.0 = true;
            Poll::Ready(Err(io::Error::new(io::ErrorKind::Interrupted, "again")))
        }
    }
    let (tx, _rx) = mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, echo(), ());
    let result = block_on(endpoint.run_async(Failing(false), Framing::Newline));
    assert!(matches!(&result, Err(Error::Io(e)) if e.kind() == io::ErrorKind::ConnectionReset));
    assert!(endpoint.is_closed());
}

#[test]
fn test_call_is_a_future_answered_from_another_thread() {
    let (tx, rx) = mpsc::channel::<String>();
    let mut endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint.peer().request("ask", json!([1]));
    let Ok(call) = call else {
        panic!("request failed")
    };
    let answering = thread::spawn(move || {
        let _request = rx.recv_timeout(Duration::from_secs(5));
        thread::sleep(Duration::from_millis(20));
        endpoint
            .handle(br#"{"jsonrpc":"2.0","id":1,"result":"yes"}"#)
            .is_ok()
    });
    let outcome = block_on(call);
    assert_eq!(
        outcome.map(|r| r.as_str().to_owned()),
        Ok("\"yes\"".to_owned())
    );
    assert!(answering.join().unwrap_or(false));
}

#[test]
fn test_awaited_call_times_out_on_its_own() {
    let (tx, _rx) = mpsc::channel::<String>();
    let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint
        .peer()
        .request_with_timeout("slow", (), Duration::from_millis(20));
    let Ok(call) = call else {
        panic!("request failed")
    };
    // Nothing else happens: the timer thread ends it and wakes the task.
    assert_eq!(block_on(call), Err(CallError::TimedOut));
}

#[test]
fn test_polling_a_finished_call_again_reads_closed() {
    let (tx, _rx) = mpsc::channel::<String>();
    let endpoint = Endpoint::new(tx, Router::<()>::new(), ());
    let call = endpoint.peer().request("x", ());
    let Ok(call) = call else {
        panic!("request failed")
    };
    assert!(call.canceller().cancel());
    let mut call = pin!(call);
    assert_eq!(block_on(call.as_mut()), Err(CallError::Cancelled));
    assert_eq!(block_on(call.as_mut()), Err(CallError::Closed));
}

#[test]
fn test_outbox_reads_as_an_async_buf_read() {
    let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    assert!(endpoint.peer().notify("one", ()).is_ok());
    drop(endpoint);
    let mut read = Vec::new();
    let mut buf = [0u8; 4];
    loop {
        let n = block_on(std::future::poll_fn(|cx| {
            Pin::new(&mut outbox).poll_read(cx, &mut buf)
        }));
        match n {
            Ok(0) => break,
            Ok(n) => read.extend_from_slice(&buf[..n]),
            Err(error) => panic!("{error}"),
        }
    }
    assert_eq!(read, b"{\"jsonrpc\":\"2.0\",\"method\":\"one\"}\n");

    let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    assert!(endpoint.peer().notify("two", ()).is_ok());
    let filled = block_on(std::future::poll_fn(|cx| {
        Pin::new(&mut outbox)
            .poll_fill_buf(cx)
            .map(|r| r.map(<[u8]>::len))
    }));
    assert!(matches!(filled, Ok(33)), "{filled:?}");
    Pin::new(&mut outbox).consume(33);
    assert_eq!(outbox.pending_bytes(), 33); // written once the next poll sees it
}

#[test]
fn test_write_to_a_failing_writer_closes_the_endpoint() {
    struct Refuses(io::ErrorKind);
    impl AsyncWrite for Refuses {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            match self.0 {
                io::ErrorKind::WriteZero => Poll::Ready(Ok(0)),
                kind => Poll::Ready(Err(io::Error::new(kind, "refused"))),
            }
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    for kind in [io::ErrorKind::WriteZero, io::ErrorKind::BrokenPipe] {
        let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
        let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
        assert!(endpoint.peer().notify("x", ()).is_ok());
        let result = block_on(outbox.write_to(Refuses(kind)));
        assert_eq!(result.err().map(|e| e.kind()), Some(kind));
        assert!(matches!(endpoint.peer().notify("y", ()), Err(Error::Io(_))));
        assert!(endpoint.is_closed());
    }
}
