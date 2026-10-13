//! The serving helpers: `Server` over TCP (and Unix domain sockets on Unix),
//! with bounded connections and a stopper.
#![cfg(feature = "net")]

use std::{
    io::{self, BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use rpc_io::{Framing, Router, Server};

fn listener() -> (TcpListener, std::net::SocketAddr) {
    let listener = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener,
        Err(error) => panic!("{error}"),
    };
    let addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(error) => panic!("{error}"),
    };
    (listener, addr)
}

fn ask(client: &mut TcpStream, reader: &mut BufReader<TcpStream>, body: &str) -> String {
    assert!(client.write_all(body.as_bytes()).is_ok());
    assert!(client.write_all(b"\n").is_ok());
    let mut line = String::new();
    assert!(reader.read_line(&mut line).is_ok());
    line
}

fn connect(addr: std::net::SocketAddr) -> (TcpStream, BufReader<TcpStream>) {
    let client = match TcpStream::connect(addr) {
        Ok(client) => client,
        Err(error) => panic!("{error}"),
    };
    let _timeout = client.set_read_timeout(Some(Duration::from_secs(10)));
    let reader = match client.try_clone() {
        Ok(reader) => BufReader::new(reader),
        Err(error) => panic!("{error}"),
    };
    (client, reader)
}

#[test]
fn test_tcp_server_serves_connections_with_their_own_state_and_stops() {
    let (listener, addr) = listener();
    let server = Server::new(Framing::Newline);
    let stopper = server.stopper();
    let (seen, connections) = mpsc::channel();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, move |connection| {
            let _sent = seen.send(connection.clone());
            let router = Router::new().request("whoami", |me: &mut u64, _, _| Ok(*me));
            (router, connection.id)
        })
    });
    let mut clients: Vec<_> = (0..3).map(|_| connect(addr)).collect();
    let mut answers = Vec::new();
    for (client, reader) in &mut clients {
        answers.push(ask(
            client,
            reader,
            r#"{"jsonrpc":"2.0","id":1,"method":"whoami"}"#,
        ));
    }
    answers.sort();
    assert_eq!(
        answers,
        [
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":0}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":1}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":2}\n",
        ]
    );
    let first = connections.recv_timeout(Duration::from_secs(5));
    assert!(first.is_ok_and(|c| c.id == 0 && c.addr.starts_with("127.0.0.1:")));
    let started = Instant::now();
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "stopping took {:?}",
        started.elapsed()
    );
    // Every client sees its stream end.
    for (_client, reader) in &mut clients {
        let mut rest = String::new();
        assert!(matches!(reader.read_line(&mut rest), Ok(0)), "{rest}");
    }
    assert!(stopper.is_stopped());
}

#[test]
fn test_tcp_server_bounds_its_connections() {
    let (listener, addr) = listener();
    let server = Server::new(Framing::Newline).with_max_connections(1);
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, |_| {
            (
                Router::new().request("ping", |_: &mut (), _, _| Ok("pong")),
                (),
            )
        })
    });
    let (mut a, mut a_reader) = connect(addr);
    assert!(
        ask(
            &mut a,
            &mut a_reader,
            r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#
        )
        .contains("pong")
    );
    // The second connection waits in the backlog: no answer while `a` is open.
    let (mut b, b_reader) = connect(addr);
    assert!(
        b.write_all(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n")
            .is_ok()
    );
    let (answered, answer) = mpsc::channel();
    let waiting = thread::spawn(move || {
        let mut reader = b_reader;
        let mut line = String::new();
        let _read = reader.read_line(&mut line);
        let _sent = answered.send(line);
    });
    assert!(answer.recv_timeout(Duration::from_millis(300)).is_err());
    drop((a, a_reader));
    let line = answer.recv_timeout(Duration::from_secs(10));
    assert!(line.is_ok_and(|l| l.contains("pong")));
    assert!(waiting.join().is_ok());
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
}

#[test]
fn test_server_stopped_before_it_serves_returns_at_once() {
    let (listener, _addr) = listener();
    let server = Server::new(Framing::ContentLength);
    server.stopper().stop();
    let started = Instant::now();
    assert!(
        server
            .serve_tcp(listener, |_| (Router::<()>::new(), ()))
            .is_ok()
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(server.connections(), 0);
    let configured = Server::new(Framing::Newline)
        .with_config(rpc_io::Config::new().with_limits(rpc_io::Limits::network()))
        .with_queue(rpc_io::QueueLimits::new().with_max_bytes(1 << 20))
        .with_max_connections(0);
    assert_eq!(configured.max_connections(), 1);
    assert_eq!(configured.config().limits, rpc_io::Limits::network());
    assert_eq!(configured.queue().max_bytes, 1 << 20);
    let timed = Server::new(Framing::Newline)
        .with_message_timeout(Some(Duration::from_secs(5)))
        .with_idle_timeout(None);
    assert_eq!(timed.message_timeout(), Some(Duration::from_secs(5)));
    assert_eq!(timed.idle_timeout(), None);
    // The defaults are sized for untrusted peers.
    assert_eq!(server.config().limits, rpc_io::Limits::network());
    assert_eq!(server.queue().max_bytes, 8 << 20);
    assert_eq!(server.message_timeout(), Some(Duration::from_secs(30)));
    assert_eq!(server.idle_timeout(), Some(Duration::from_secs(300)));
    assert!(format!("{server:?} {:?}", server.stopper()).contains("max_connections: 256"));
}

#[cfg(unix)]
#[test]
fn test_unix_server_serves_and_stops() {
    use std::os::unix::net::{UnixListener, UnixStream};
    let path = std::env::temp_dir().join(format!("rpc-io-test-{}.sock", std::process::id()));
    let _stale = std::fs::remove_file(&path);
    let listener = match UnixListener::bind(&path) {
        Ok(listener) => listener,
        Err(error) => panic!("{error}"),
    };
    let server = Server::new(Framing::Newline);
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_unix(listener, |_| {
            (
                Router::new().request("ping", |_: &mut (), _, _| Ok("pong")),
                (),
            )
        })
    });
    let mut client = match UnixStream::connect(&path) {
        Ok(client) => client,
        Err(error) => panic!("{error}"),
    };
    assert!(
        client
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"ping\"}\n")
            .is_ok()
    );
    let mut line = String::new();
    let reader = client.try_clone().map(BufReader::new);
    assert!(reader.is_ok_and(|mut r| r.read_line(&mut line).is_ok()));
    assert_eq!(line, "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n");
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
    let _removed = std::fs::remove_file(&path);
}

#[test]
fn test_silent_sockets_cannot_lock_out_a_client() {
    // Found by the verifier (RPC2-002): four sockets that connected and then
    // sent one byte (or nothing) held every slot of a 4-slot server forever.
    let (listener, addr) = listener();
    let server = Server::new(Framing::Newline)
        .with_max_connections(4)
        .with_message_timeout(Some(Duration::from_millis(300)));
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, |_| {
            (
                Router::new().request("ping", |_: &mut (), _, _| Ok("pong")),
                (),
            )
        })
    });
    let mut silent = Vec::new();
    for i in 0..4 {
        let (mut socket, _reader) = connect(addr);
        if i % 2 == 0 {
            assert!(socket.write_all(b"{").is_ok()); // a message begun, never finished
        }
        silent.push(socket);
    }
    let started = Instant::now();
    let (mut client, mut reader) = connect(addr);
    let answer = ask(
        &mut client,
        &mut reader,
        r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#,
    );
    assert_eq!(
        answer,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"pong\"}\n"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    // The silent ones were closed.
    for socket in &mut silent {
        let _timeout = socket.set_read_timeout(Some(Duration::from_secs(5)));
        let mut rest = Vec::new();
        assert!(matches!(io::Read::read_to_end(socket, &mut rest), Ok(0)));
    }
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
}

#[test]
fn test_a_client_streaming_past_the_message_deadline_is_served() {
    // Found by the verifier (RPC3-001): when every read ended inside the next
    // message, the message deadline never restarted, and a busy client was
    // cut off one `message_timeout` after it began.
    let (listener, addr) = listener();
    let server =
        Server::new(Framing::Newline).with_message_timeout(Some(Duration::from_millis(300)));
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, |_| {
            (
                Router::new().request("ping", |_: &mut (), _, _| Ok("pong")),
                (),
            )
        })
    });
    let (mut client, mut reader) = connect(addr);
    let count = 120;
    let wire: Vec<u8> = (0..count)
        .flat_map(|id| {
            format!(
                "{{\"jsonrpc\":\"2.0\",\"id\":{id},\"method\":\"ping\"}}
"
            )
            .into_bytes()
        })
        .collect();
    // A message and a half every 20 ms, for about 2 s: every read ends inside
    // the next message, and the stream lasts several deadlines.
    let chunk = wire.len() * 3 / (2 * count);
    let writer = thread::spawn(move || {
        for piece in wire.chunks(chunk) {
            if client.write_all(piece).is_err() {
                return false;
            }
            thread::sleep(Duration::from_millis(20));
        }
        true
    });
    let started = Instant::now();
    let mut answered = 0;
    let mut line = String::new();
    while answered < count {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(n) if n > 0 => {
                assert!(line.contains("pong"), "{line}");
                answered += 1;
            }
            other => panic!(
                "cut off after {answered} answers at {:?}: {other:?}",
                started.elapsed()
            ),
        }
    }
    assert!(writer.join().is_ok_and(|wrote| wrote));
    assert!(
        started.elapsed() > Duration::from_millis(900),
        "{:?}",
        started.elapsed()
    );
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
}

#[test]
fn test_idle_and_unfinished_connections_are_closed() {
    let (listener, addr) = listener();
    let server = Server::new(Framing::Newline)
        .with_message_timeout(Some(Duration::from_millis(400)))
        .with_idle_timeout(Some(Duration::from_millis(400)));
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, |_| {
            (
                Router::new().request("ping", |_: &mut (), _, _| Ok("pong")),
                (),
            )
        })
    });
    // Idle after a message: served, then closed once silent too long.
    let (mut client, mut reader) = connect(addr);
    assert!(
        ask(
            &mut client,
            &mut reader,
            r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#
        )
        .contains("pong")
    );
    let started = Instant::now();
    let mut rest = String::new();
    assert!(matches!(reader.read_line(&mut rest), Ok(0)));
    let idle = started.elapsed();
    assert!(
        idle >= Duration::from_millis(300) && idle < Duration::from_secs(5),
        "{idle:?}"
    );
    // A message trickled a byte at a time, but too slowly: closed even
    // though bytes keep arriving.
    let (mut slow, mut slow_reader) = connect(addr);
    let message = br#"{"jsonrpc":"2.0","id":2,"method":"ping"}"#;
    let trickle = thread::spawn(move || {
        for byte in message.iter() {
            if slow.write_all(&[*byte]).is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        let _end = slow.write_all(b"\n");
    });
    let mut line = String::new();
    assert!(matches!(slow_reader.read_line(&mut line), Ok(0)), "{line}");
    assert!(trickle.join().is_ok());
    stopper.stop();
    assert!(serving.join().is_ok_and(|served| served.is_ok()));
}
