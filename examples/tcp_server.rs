//! A JSON-RPC service on a TCP port: one endpoint per connection with its own
//! state, the network limits, an interceptor that refuses calls until the
//! client logs in, and a clean stop. A client in the same program drives it.
//!
//! ```text
//! $ cargo run --example tcp_server --features net
//! ```

use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    thread,
};

use rpc_io::{Config, ErrorCode, ErrorObject, Framing, Limits, Router, Server};

/// One connection's state.
#[derive(Default)]
struct Session {
    user: Option<String>,
    calls: u64,
}

fn router() -> Router<Session> {
    Router::new()
        .request("login", |session: &mut Session, params, _| {
            let p = params.parse()?;
            let user = p["user"]
                .as_str()
                .ok_or_else(|| ErrorObject::invalid_params("expected {\"user\": ...}"))?;
            session.user = Some(user.to_owned());
            Ok(true)
        })
        .request("whoami", |session, _, _| {
            Ok(session.user.clone().unwrap_or_default())
        })
        .request("calls", |session, _, _| Ok(session.calls))
        .intercept(|session, method, _id, _params| {
            session.calls += 1;
            if session.user.is_none() && method != "login" {
                return Err(ErrorObject::new(ErrorCode(1003), "log in first"));
            }
            Ok(())
        })
}

fn main() -> std::io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    let server = Server::new(Framing::Newline)
        .with_config(Config::new().with_limits(Limits::network()))
        .with_max_connections(64);
    let stopper = server.stopper();
    let serving = thread::spawn(move || {
        server.serve_tcp(listener, |connection| {
            eprintln!("connection {} from {}", connection.id, connection.addr);
            (router(), Session::default())
        })
    });
    println!("listening on {addr}");

    let client = TcpStream::connect(addr)?;
    let mut writer = client.try_clone()?;
    let mut reader = BufReader::new(client);
    for request in [
        r#"{"jsonrpc":"2.0","id":1,"method":"whoami"}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"login","params":{"user":"ada"}}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"whoami"}"#,
        r#"{"jsonrpc":"2.0","id":4,"method":"calls"}"#,
    ] {
        writer.write_all(request.as_bytes())?;
        writer.write_all(b"\n")?;
        let mut answer = String::new();
        reader.read_line(&mut answer)?;
        print!("{request}\n  -> {answer}");
    }

    stopper.stop();
    serving
        .join()
        .map_err(|_| std::io::Error::other("the server thread panicked"))??;
    println!("stopped");
    Ok(())
}
