//! [`Sink`]: where an endpoint's outgoing messages go, and [`Framed`], the
//! sink that frames them onto a byte stream.

use std::{io, io::Write, sync::mpsc};

use crate::framing::Framing;

/// Where an endpoint sends its messages.
///
/// Each call to [`send`](Sink::send) is one complete message body (a message
/// or a batch, compact JSON, no framing); the sink frames and transmits it.
/// [`flush`](Sink::flush) is called after each incoming message has been
/// handled, and after each message sent from another thread, so a sink may
/// buffer within a turn and write once.
///
/// The endpoint calls the sink with its internal lock held, from whichever
/// thread sends: a sink must not call back into the endpoint or its handles.
///
/// Implemented for [`Framed`] (a byte stream) and `mpsc::Sender<String>`
/// (one `String` per message, for message-oriented transports and tests).
///
/// # Examples
///
/// A sink that counts messages:
///
/// ```
/// use std::io;
/// use rpc_io::Sink;
///
/// struct Count(usize);
///
/// impl Sink for Count {
///     fn send(&mut self, _message: &str) -> io::Result<()> {
///         self.0 += 1;
///         Ok(())
///     }
/// }
/// ```
pub trait Sink: Send + 'static {
    /// Sends one message body.
    ///
    /// # Errors
    ///
    /// Any I/O error. The endpoint then closes: the error ends
    /// [`Endpoint::run`](crate::Endpoint::run) and every later send fails with
    /// [`Error::Closed`](crate::Error::Closed).
    fn send(&mut self, message: &str) -> io::Result<()>;

    /// Transmits anything buffered. The default does nothing.
    ///
    /// # Errors
    ///
    /// Any I/O error, with the same effect as from [`send`](Sink::send).
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Sink for mpsc::Sender<String> {
    fn send(&mut self, message: &str) -> io::Result<()> {
        mpsc::Sender::send(self, message.to_owned()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "the receiving end of the message channel is gone",
            )
        })
    }
}

/// Above this many buffered bytes a turn's output is written before the turn
/// ends, so a handler that sends a great deal holds a bounded buffer.
const WRITE_AT: usize = 64 * 1024;

/// A [`Sink`] that frames messages onto a byte stream: stdout, a socket, a
/// pipe.
///
/// Messages are framed into a reused buffer and written with one `write_all`
/// and one `flush` per turn (and whenever more than 64 KiB is waiting), so a
/// handler that sends ten notifications costs one system call, not ten.
///
/// # Examples
///
/// ```
/// use rpc_io::{Framed, Framing, Sink};
///
/// let mut framed = Framed::new(Vec::new(), Framing::ContentLength);
/// framed.send(r#"{"jsonrpc":"2.0","method":"x"}"#)?;
/// assert!(framed.get_ref().is_empty()); // buffered until the turn ends
/// framed.flush()?;
/// assert_eq!(framed.get_ref().as_slice(), &b"Content-Length: 30\r\n\r\n{\"jsonrpc\":\"2.0\",\"method\":\"x\"}"[..]);
/// # Ok::<(), std::io::Error>(())
/// ```
#[derive(Debug)]
pub struct Framed<W> {
    writer: W,
    framing: Framing,
    out: Vec<u8>,
}

impl<W: Write> Framed<W> {
    /// Frames messages with `framing` onto `writer`.
    ///
    /// # Examples
    ///
    /// ```
    /// let framed = rpc_io::Framed::new(std::io::sink(), rpc_io::Framing::Newline);
    /// assert_eq!(framed.framing(), rpc_io::Framing::Newline);
    /// ```
    #[must_use]
    pub fn new(writer: W, framing: Framing) -> Self {
        Self {
            writer,
            framing,
            out: Vec::new(),
        }
    }

    /// The framing.
    ///
    /// # Examples
    ///
    /// ```
    /// let framed = rpc_io::Framed::new(Vec::new(), rpc_io::Framing::ContentLength);
    /// assert_eq!(framed.framing(), rpc_io::Framing::ContentLength);
    /// ```
    #[inline]
    #[must_use]
    pub const fn framing(&self) -> Framing {
        self.framing
    }

    /// The writer.
    ///
    /// # Examples
    ///
    /// ```
    /// let framed = rpc_io::Framed::new(Vec::<u8>::new(), rpc_io::Framing::Newline);
    /// assert!(framed.get_ref().is_empty());
    /// ```
    #[inline]
    #[must_use]
    pub const fn get_ref(&self) -> &W {
        &self.writer
    }

    /// The writer, mutably. Bytes written to it directly may interleave with
    /// framed messages.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut framed = rpc_io::Framed::new(Vec::<u8>::new(), rpc_io::Framing::Newline);
    /// framed.get_mut().clear();
    /// ```
    #[inline]
    pub fn get_mut(&mut self) -> &mut W {
        &mut self.writer
    }

    /// The writer, dropping any messages not yet flushed.
    ///
    /// # Examples
    ///
    /// ```
    /// let framed = rpc_io::Framed::new(Vec::<u8>::new(), rpc_io::Framing::Newline);
    /// assert!(framed.into_inner().is_empty());
    /// ```
    #[must_use]
    pub fn into_inner(self) -> W {
        self.writer
    }

    fn write_out(&mut self) -> io::Result<()> {
        if self.out.is_empty() {
            return Ok(());
        }
        let result = self.writer.write_all(&self.out);
        self.out.clear();
        result
    }
}

impl<W: Write + Send + 'static> Sink for Framed<W> {
    fn send(&mut self, message: &str) -> io::Result<()> {
        self.framing.encode(message.as_bytes(), &mut self.out);
        if self.out.len() >= WRITE_AT {
            self.write_out()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.write_out()?;
        self.writer.flush()
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_framed_writes_large_turns_early() {
        let mut framed = Framed::new(Vec::new(), Framing::Newline);
        let big = "x".repeat(WRITE_AT);
        assert!(framed.send(&big).is_ok());
        assert_eq!(framed.get_ref().len(), WRITE_AT + 1);
        assert!(framed.out.is_empty());
    }

    #[test]
    fn test_channel_sink_reports_a_dropped_receiver() {
        let (mut tx, rx) = mpsc::channel::<String>();
        assert!(Sink::send(&mut tx, "a").is_ok());
        assert_eq!(rx.recv().ok().as_deref(), Some("a"));
        drop(rx);
        let error = Sink::send(&mut tx, "b").err().map(|e| e.kind());
        assert_eq!(error, Some(io::ErrorKind::BrokenPipe));
    }
}
