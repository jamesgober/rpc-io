//! The `tracing` feature: spans for calls, events for what the endpoint does
//! on its own, and nothing from `params`. Alone in its test binary: `tracing`
//! caches each call site's interest process-wide.
#![cfg(feature = "tracing")]

mod common;

use std::{
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use common::{endpoint, quiet_deliberate_panics};
use rpc_io::{CallError, Config, Responder, Router};
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};

/// Records the message of every event and the name of every span.
#[derive(Default)]
struct Recorder {
    next: AtomicU64,
    lines: Mutex<Vec<String>>,
}

struct Message<'a>(&'a mut String);

impl Visit for Message<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!(" {}={value:?}", field.name()));
    }
}

/// The subscriber: a handle on the shared recorder.
struct Handle(Arc<Recorder>);

impl Subscriber for Handle {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }
    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut line = format!("span {}", span.metadata().name());
        span.record(&mut Message(&mut line));
        self.0
            .lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
        Id::from_u64(self.0.next.fetch_add(1, Ordering::Relaxed) + 1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut line = format!("{} {}", event.metadata().level(), event.metadata().target());
        event.record(&mut Message(&mut line));
        self.0
            .lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(line);
    }
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

#[test]
fn test_tracing_records_calls_and_events_without_params() {
    quiet_deliberate_panics();
    let recorder = Arc::new(Recorder::default());
    let lines = tracing::subscriber::with_default(Handle(Arc::clone(&recorder)), || {
        let (e, _rx) = endpoint(router(), Vec::new());
        // Deadlines expire here, on the recorded thread, not the timer's.
        let mut e = e.with_config(Config::new().with_timer(false));
        for body in [
            &br#"{"jsonrpc":"2.0","id":1,"method":"ok","params":{"secret":"hunter2"}}"#[..],
            br#"{"jsonrpc":"2.0","id":2,"method":"boom"}"#,
            br#"{"jsonrpc":"2.0","method":"nobody"}"#,
            b"{oops",
        ] {
            assert!(e.handle(body).is_ok());
        }
        let call = e.peer().request_with_timeout("slow", (), Duration::ZERO);
        e.expire();
        assert!(call.is_ok_and(|c| c.wait() == Err(CallError::TimedOut)));
        e.close();
        recorder
            .lines
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    });
    let all = lines.join("\n");
    assert!(all.contains("span request method=ok id=1"), "{all}");
    assert!(all.contains("span notification method=nobody"), "{all}");
    assert!(
        all.contains("ERROR rpc_io") && all.contains("a handler panicked"),
        "{all}"
    );
    assert!(all.contains("a notification with no handler"), "{all}");
    assert!(all.contains("a malformed message"), "{all}");
    assert!(all.contains("our request timed out"), "{all}");
    assert!(all.contains("the connection is closed"), "{all}");
    assert!(!all.contains("hunter2"), "params were recorded: {all}");
}

fn router() -> Router<Vec<Responder>> {
    Router::<Vec<Responder>>::new()
        .request("ok", |_, _, _| Ok(1))
        .request("boom", |_, _, _| -> Result<(), rpc_io::ErrorObject> {
            panic!("deliberate: tracing")
        })
}
