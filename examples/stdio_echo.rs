//! A JSON-RPC server on stdin and stdout: the shape of an MCP server (newline
//! framing, the default) or an LSP or DAP server (`--content-length`).
//!
//! ```text
//! $ echo '{"jsonrpc":"2.0","id":1,"method":"echo","params":["hi"]}' | cargo run --example stdio_echo
//! {"jsonrpc":"2.0","id":1,"result":["hi"]}
//! ```
//!
//! Methods: `echo` (returns its params), `ping` (returns `"pong"`), and the
//! notification `exit` (stops the server). Stdout carries the protocol, so
//! anything else goes to stderr.

use rpc_io::{End, ErrorObject, Framing, Raw, Router};

fn main() -> Result<(), rpc_io::Error> {
    let framing = if std::env::args().any(|a| a == "--content-length") {
        Framing::ContentLength
    } else {
        Framing::Newline
    };
    let router = Router::new()
        .request("echo", |_, params, _| {
            // Forwarded as the exact text received: no parse, no re-serialization.
            Ok(params.into_raw().map_or(Raw::NULL, Raw::into_owned))
        })
        .request("ping", |_, params, _| {
            if params.is_present() {
                return Err(ErrorObject::invalid_params("`ping` takes no params"));
            }
            Ok("pong")
        })
        .notification("exit", |_, _, cx| cx.stop());

    let end = rpc_io::serve(std::io::stdin().lock(), std::io::stdout(), framing, router)?;
    match end {
        End::Stopped => eprintln!("stdio_echo: stopped by `exit`"),
        _ => eprintln!("stdio_echo: input ended"),
    }
    Ok(())
}
