//! The async adapter, over `futures-io`'s `AsyncRead`, `AsyncBufRead`, and
//! `AsyncWrite` (the `futures-io` feature).
//!
//! Runtime-agnostic: smol and async-std implement these traits directly, and
//! tokio's types get them through `tokio-util`'s `compat` module. Without the
//! feature, the same loop is a few lines over [`Endpoint::next_read`] and
//! [`Outbox::poll_chunk`], which need no traits at all.

use std::{
    future::poll_fn,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_io::{AsyncBufRead, AsyncRead, AsyncWrite};

use crate::{
    endpoint::{End, Endpoint, Next},
    error::Error,
    framing::Framing,
    queue::Outbox,
    sink::Sink,
};

impl<S, K: Sink> Endpoint<S, K> {
    /// [`run`](Endpoint::run) for an async byte stream: reads framed messages
    /// from `input` and handles each, until the input ends, a handler stops
    /// the endpoint, or the connection fails.
    ///
    /// Handlers run inside this future, on whatever thread polls it, so they
    /// must not block: answer slow requests from a
    /// [deferred](crate::Router::deferred) handler's worker. Pair it with an
    /// [`outbox`](crate::outbox) for the output, written by a task of its
    /// own ([`Outbox::write_to`]).
    ///
    /// # Errors
    ///
    /// As [`run`](Endpoint::run).
    ///
    /// # Examples
    ///
    /// On any executor; here, a minimal one:
    ///
    /// ```
    /// use std::{future::Future, pin::pin, sync::Arc, task::{Context, Poll, Wake, Waker}};
    /// use rpc_io::{End, Endpoint, Framing, QueueLimits, Router};
    ///
    /// fn block_on<F: Future>(future: F) -> F::Output {
    ///     struct Unpark(std::thread::Thread);
    ///     impl Wake for Unpark {
    ///         fn wake(self: Arc<Self>) { self.0.unpark() }
    ///     }
    ///     let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    ///     let mut future = pin!(future);
    ///     loop {
    ///         if let Poll::Ready(out) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
    ///             return out;
    ///         }
    ///         std::thread::park();
    ///     }
    /// }
    ///
    /// let router = Router::new().request("ping", |_: &mut (), _, _| Ok("pong"));
    /// let (sink, mut outbox) = rpc_io::outbox(Framing::Newline, QueueLimits::new());
    /// let mut endpoint = Endpoint::new(sink, router, ());
    /// // `&[u8]` stands in for a socket: `futures-io` implements `AsyncRead` for it.
    /// let input: &[u8] = b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n";
    /// assert_eq!(block_on(endpoint.run_async(input, Framing::Newline))?, End::Eof);
    ///
    /// let mut output = Vec::new(); // `futures-io` implements `AsyncWrite` for it
    /// block_on(outbox.write_to(&mut output)).map_err(rpc_io::Error::Io)?;
    /// assert_eq!(output, b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
    /// # Ok::<(), rpc_io::Error>(())
    /// ```
    pub async fn run_async<R: AsyncRead + Unpin>(
        &mut self,
        mut input: R,
        framing: Framing,
    ) -> Result<End, Error> {
        loop {
            let buf = match self.next_read(framing)? {
                Next::Read(buf) => buf,
                Next::Stopped => return Ok(End::Stopped),
            };
            let read = poll_fn(|cx| Pin::new(&mut input).poll_read(cx, &mut buf[..])).await;
            match read {
                Ok(0) => return self.finish(),
                Ok(n) => self.commit(n),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.close();
                    return Err(Error::Io(error));
                }
            }
        }
    }
}

impl Outbox {
    /// Writes everything the outbox yields to `writer`, flushing after each
    /// batch, until the endpoint closes and all of it is written. Run it as
    /// its own task.
    ///
    /// # Errors
    ///
    /// The writer's I/O error, or [`io::ErrorKind::WriteZero`] if it accepts
    /// no bytes. The queue fails with it too, so the endpoint closes.
    ///
    /// # Examples
    ///
    /// See [`Endpoint::run_async`].
    pub async fn write_to<W: AsyncWrite + Unpin>(&mut self, mut writer: W) -> io::Result<()> {
        loop {
            let ready = poll_fn(|cx| self.poll_chunk(cx).map(<[u8]>::len)).await;
            if ready == 0 {
                return poll_fn(|cx| Pin::new(&mut writer).poll_flush(cx)).await;
            }
            while !self.current().is_empty() {
                let written =
                    poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, self.current())).await;
                match written {
                    Ok(0) => {
                        return Err(self.abandon(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "the writer accepted no bytes",
                        )));
                    }
                    Ok(n) => self.consume(n),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                    Err(error) => return Err(self.abandon(error)),
                }
            }
            if let Err(error) = poll_fn(|cx| Pin::new(&mut writer).poll_flush(cx)).await {
                return Err(self.abandon(error));
            }
        }
    }
}

impl AsyncRead for Outbox {
    /// Copies the next framed bytes into `buf`; `Ok(0)` at the end.
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let chunk = match this.poll_chunk(cx) {
            Poll::Ready(chunk) => chunk,
            Poll::Pending => return Poll::Pending,
        };
        let n = chunk.len().min(buf.len());
        buf[..n].copy_from_slice(&chunk[..n]);
        this.consume(n);
        Poll::Ready(Ok(n))
    }
}

impl AsyncBufRead for Outbox {
    /// As [`Outbox::poll_chunk`]: an empty buffer at the end.
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        self.get_mut().poll_chunk(cx).map(Ok)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Outbox::consume(self.get_mut(), amt);
    }
}
