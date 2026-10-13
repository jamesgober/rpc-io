//! The outgoing path: a peer that stops reading must not stall the endpoint
//! or its handles, and must cost bounded memory and time.
#![cfg(feature = "std")]

mod common;

use std::{
    io::{self, Write},
    sync::{Arc, mpsc},
    task::{Context, Poll, Wake, Waker},
    thread,
    time::{Duration, Instant},
};

use common::{Shared, pipe};
use rpc_io::{
    CallError, End, Endpoint, Error, Framed, Framing, QueueLimits, Queued, Router, outbox,
};

/// A writer that blocks every write until released, like a peer that has
/// stopped reading from a full pipe.
struct Stalled {
    release: mpsc::Receiver<()>,
    written: Shared,
}

impl Write for Stalled {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.release.recv().is_err() {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "released"));
        }
        self.written.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn stalled(limits: QueueLimits) -> (Queued, mpsc::Sender<()>, Shared) {
    let (release, wait) = mpsc::channel();
    let written = Shared::default();
    let writer = Stalled {
        release: wait,
        written: written.clone(),
    };
    match Queued::with_limits(writer, Framing::Newline, limits) {
        Ok(sink) => (sink, release, written),
        Err(error) => panic!("{error}"),
    }
}

/// Runs `f` on another thread; `None` if it does not return within `limit`.
fn within<T: Send + 'static>(limit: Duration, f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    let (done, result) = mpsc::channel();
    let _worker = thread::spawn(move || done.send(f()));
    result.recv_timeout(limit).ok()
}

#[test]
fn test_stalled_peer_does_not_stall_the_handles() {
    // Found by the audit (RPC-SEC-01, probe P4): with the write under the
    // endpoint's lock, a peer that stopped reading froze every handle.
    let (sink, release, written) = stalled(QueueLimits::new());
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    let peer = endpoint.peer();
    assert!(peer.notify("first", ()).is_ok()); // the writer is now stuck on it
    let worker = peer.clone();
    let sent = within(Duration::from_secs(2), move || {
        (0..100).all(|_| worker.notify("more", ()).is_ok()) && !worker.is_closed()
    });
    assert_eq!(
        sent,
        Some(true),
        "a handle blocked behind the stalled write"
    );
    for _ in 0..200 {
        let _go = release.send(());
    }
    drop(endpoint);
    drop(peer);
    let text = written.text();
    assert_eq!(text.lines().count(), 101, "{text}");
    assert!(text.starts_with("{\"jsonrpc\":\"2.0\",\"method\":\"first\"}\n"));
}

#[test]
fn test_is_closed_does_not_wait_for_a_blocked_sink() {
    // Even with a sink that writes under the lock, `is_closed` reads a flag.
    struct Blocking(mpsc::Receiver<()>);
    impl Write for Blocking {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let _woken = self.0.recv();
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let (release, wait) = mpsc::channel();
    let endpoint = Endpoint::new(
        Framed::new(Blocking(wait), Framing::Newline),
        Router::<()>::new(),
        (),
    );
    let peer = endpoint.peer();
    let sender = peer.clone();
    let _blocked = thread::spawn(move || sender.notify("stuck", ()));
    thread::sleep(Duration::from_millis(50));
    let watcher = peer.clone();
    assert_eq!(
        within(Duration::from_secs(2), move || watcher.is_closed()),
        Some(false)
    );
    drop(release);
}

#[test]
fn test_overflowing_queue_closes_the_connection() {
    let limits = QueueLimits::new().with_max_messages(4);
    let (sink, release, _written) = stalled(limits);
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    let peer = endpoint.peer();
    let call = peer.request("pending", ());
    let Ok(call) = call else {
        panic!("request failed")
    };
    let mut results = Vec::new();
    for _ in 0..10 {
        results.push(peer.notify("n", ()).map_err(|e| e.to_string()));
    }
    assert!(results[..3].iter().all(Result::is_ok), "{results:?}");
    assert!(
        results[3]
            .as_ref()
            .is_err_and(|e| e.contains("the output queue is full")),
        "{results:?}"
    );
    assert!(peer.is_closed());
    assert!(matches!(peer.notify("late", ()), Err(Error::Closed)));
    assert_eq!(call.wait(), Err(CallError::Closed));
    drop(release);
}

#[test]
fn test_stalled_write_times_out_and_closes() {
    let limits = QueueLimits::new().with_write_timeout(Some(Duration::from_millis(30)));
    let (sink, release, _written) = stalled(limits);
    let mut endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    assert!(endpoint.peer().notify("stuck", ()).is_ok());
    thread::sleep(Duration::from_millis(80));
    let result = endpoint.peer().notify("next", ());
    assert!(
        matches!(&result, Err(Error::Io(e)) if e.kind() == io::ErrorKind::TimedOut),
        "{result:?}"
    );
    assert!(endpoint.is_closed());
    // Closing again does not wait on the stuck writer.
    let started = Instant::now();
    endpoint.close();
    drop(endpoint);
    assert!(started.elapsed() < Duration::from_secs(5));
    drop(release);
}

#[test]
fn test_serve_writes_everything_before_returning() {
    let (to_server, server_in) = pipe();
    let (to_client, mut client_in) = pipe();
    let server = thread::spawn(move || {
        let router = Router::new()
            .request("echo", |_: &mut (), params, _| {
                Ok(params
                    .into_raw()
                    .map_or(rpc_io::Raw::NULL, rpc_io::Raw::into_owned))
            })
            .notification("exit", |_, _, cx| cx.stop());
        rpc_io::serve(server_in, to_client, Framing::Newline, router)
    });
    let mut to_server = to_server;
    let mut wire = String::new();
    for i in 0..500 {
        wire.push_str(&format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":{i},\"method\":\"echo\",\"params\":[{i}]}}\n"
        ));
    }
    wire.push_str("{\"jsonrpc\":\"2.0\",\"method\":\"exit\"}\n");
    assert!(to_server.write_all(wire.as_bytes()).is_ok());
    assert!(matches!(server.join().ok(), Some(Ok(End::Stopped))));
    // `serve` has returned: its output is complete and the stream has ended.
    let mut answers = String::new();
    assert!(io::Read::read_to_string(&mut client_in, &mut answers).is_ok());
    assert_eq!(answers.lines().count(), 500);
    assert!(answers.ends_with("{\"jsonrpc\":\"2.0\",\"id\":499,\"result\":[499]}\n"));
}

#[test]
fn test_outbox_carries_an_endpoint_and_ends_when_it_closes() {
    let mut cx = Context::from_waker(Waker::noop());
    let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    let (sink, mut outbox) = outbox(Framing::ContentLength, QueueLimits::new());
    let mut endpoint = Endpoint::new(sink, router, ());
    assert!(
        endpoint
            .handle(br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#)
            .is_ok()
    );
    let mut wire = Vec::new();
    let mut take = |outbox: &mut rpc_io::Outbox, wire: &mut Vec<u8>| loop {
        match outbox.poll_chunk(&mut cx) {
            Poll::Ready([]) => return true,
            Poll::Ready(chunk) => {
                let n = chunk.len().min(7); // a writer that takes 7 bytes at a time
                wire.extend_from_slice(&chunk[..n]);
                outbox.consume(n);
            }
            Poll::Pending => return false,
        }
    };
    assert!(!take(&mut outbox, &mut wire));
    assert_eq!(
        wire,
        b"Content-Length: 40\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}"
    );
    endpoint.close();
    assert!(take(&mut outbox, &mut wire), "the outbox did not end");
}

#[test]
fn test_dropped_outbox_closes_the_endpoint() {
    let (sink, outbox) = outbox(Framing::Newline, QueueLimits::new());
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    drop(outbox);
    assert!(matches!(endpoint.peer().notify("x", ()), Err(Error::Io(_))));
    assert!(endpoint.is_closed());
}

#[test]
fn test_outbox_task_is_woken_by_a_message_and_by_the_close() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            let _previous = self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let (sink, mut outbox) = outbox(Framing::Newline, QueueLimits::new());
    let endpoint = Endpoint::new(sink, Router::<()>::new(), ());
    assert!(outbox.poll_chunk(&mut cx).is_pending());
    assert!(endpoint.peer().notify("x", ()).is_ok());
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
    assert!(matches!(outbox.poll_chunk(&mut cx), Poll::Ready(chunk) if !chunk.is_empty()));
    outbox.consume(usize::MAX);
    assert!(outbox.poll_chunk(&mut cx).is_pending());
    drop(endpoint);
    assert_eq!(count.0.load(Ordering::SeqCst), 2);
    assert!(matches!(outbox.poll_chunk(&mut cx), Poll::Ready([])));
}
