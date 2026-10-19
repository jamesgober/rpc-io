//! A peer may reuse a request's id once that request has its response (or,
//! under MCP, once it cancelled it). Handles of the earlier request
//! (`Responder`, `Notifier`, `Progress`) must then never act on the later
//! one: each is bound to its own request, not to the id (the 0.4.0 release
//! verification, RPC5-001 and RPC5-008; the `Responder` case was present
//! since 0.2).
#![cfg(feature = "std")]

mod common;

use common::{endpoint, sent};
use rpc_io::{Config, Error, Notifier, Progress, Responder, Router};

#[derive(Default)]
struct Kept {
    responders: Vec<Responder>,
    notifiers: Vec<Notifier>,
    progress: Vec<Progress>,
}

fn router() -> Router<Kept> {
    Router::new().deferred("work", |kept: &mut Kept, params, cx, responder| {
        kept.notifiers.push(responder.notifier());
        kept.progress.extend(responder.progress(&params));
        // Created separately: shares the request's history with the other.
        kept.progress.extend(cx.progress(&params));
        kept.responders.push(responder);
    })
}

fn work(id: u32, token: &str) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":{id},"method":"work","params":{{"_meta":{{"progressToken":"{token}"}}}}}}"#
    )
}

#[test]
fn test_a_stale_responder_cannot_answer_a_request_that_reused_its_id() {
    for config in [Config::new(), Config::mcp()] {
        let lsp = config
            .cancellation
            .as_ref()
            .is_some_and(|c| c.reply.is_some());
        let (e, rx) = endpoint(router(), Kept::default());
        let mut e = e.with_config(config);
        assert!(e.handle(work(5, "a").as_bytes()).is_ok());
        let cancel = if lsp {
            br#"{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":5}}"#.to_vec()
        } else {
            br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":5}}"#
                .to_vec()
        };
        assert!(e.handle(&cancel).is_ok());
        let _cancelled_answer = sent(&rx);
        // The peer reuses the id.
        assert!(e.handle(work(5, "b").as_bytes()).is_ok());
        let mut kept = std::mem::take(e.state_mut());
        let new = kept.responders.pop();
        let old = kept.responders.pop();
        // The cancelled request's late answer reaches nobody.
        assert!(matches!(
            old.map(|r| r.ok("OLD")),
            Some(Err(Error::AlreadyAnswered))
        ));
        assert!(sent(&rx).is_empty(), "the stale answer went out");
        // The new request is still in flight and gets its own answer.
        assert!(new.is_some_and(|r| r.ok("NEW").is_ok()));
        assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","id":5,"result":"NEW"}"#]);
    }
}

#[test]
fn test_dropping_a_stale_responder_leaves_the_new_request_alone() {
    let (e, rx) = endpoint(router(), Kept::default());
    let mut e = e.with_config(Config::mcp());
    assert!(e.handle(work(7, "a").as_bytes()).is_ok());
    let cancel =
        br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":7}}"#;
    assert!(e.handle(cancel).is_ok());
    assert!(e.handle(work(7, "b").as_bytes()).is_ok());
    let mut kept = std::mem::take(e.state_mut());
    let new = kept.responders.pop();
    drop(kept.responders.pop()); // the stale one, unanswered
    assert!(
        sent(&rx).is_empty(),
        "dropping the stale responder answered the new request"
    );
    assert!(new.is_some_and(|r| r.ok(1).is_ok()));
    assert_eq!(sent(&rx), [r#"{"jsonrpc":"2.0","id":7,"result":1}"#]);
}

#[test]
fn test_stale_notifiers_and_progress_stay_quiet_after_the_id_is_reused() {
    for cancelled in [false, true] {
        let (e, rx) = endpoint(router(), Kept::default());
        let mut e = e.with_config(Config::mcp());
        assert!(e.handle(work(9, "t1").as_bytes()).is_ok());
        let first = std::mem::take(e.state_mut());
        assert!(first.progress[0].report(3.0, None, None).is_ok());
        if cancelled {
            let cancel =
                br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":9}}"#;
            assert!(e.handle(cancel).is_ok());
            drop(first.responders);
        } else {
            assert!(first.responders.into_iter().all(|r| r.ok(()).is_ok()));
        }
        let _first_traffic = sent(&rx);
        assert!(e.handle(work(9, "t2").as_bytes()).is_ok());
        // Every handle of the first request refuses, and sends nothing.
        assert!(matches!(
            first.notifiers[0].notify("list_changed", ()),
            Err(Error::AlreadyAnswered)
        ));
        for progress in &first.progress {
            assert!(matches!(
                progress.report(4.0, None, None),
                Err(Error::AlreadyAnswered)
            ));
        }
        assert!(
            sent(&rx).is_empty(),
            "a stale handle sent for the new request"
        );
        // The new request's handles work, with a history of their own.
        let second = std::mem::take(e.state_mut());
        assert!(second.notifiers[0].notify("list_changed", ()).is_ok());
        assert!(second.progress[0].report(1.0, None, None).is_ok());
        let traffic = sent(&rx).concat();
        assert!(
            traffic.contains(r#""progressToken":"t2","progress":1"#),
            "{traffic}"
        );
        assert!(!traffic.contains("t1"), "{traffic}");
        drop(second.responders);
    }
}

#[test]
fn test_separately_made_progress_handles_share_their_request_history() {
    // Deferred: one from the responder, one from the context.
    let (e, rx) = endpoint(router(), Kept::default());
    let mut e = e.with_config(Config::mcp());
    assert!(e.handle(work(1, "p").as_bytes()).is_ok());
    let kept = std::mem::take(e.state_mut());
    assert_eq!(kept.progress.len(), 2);
    assert!(kept.progress[0].report(2.0, None, None).is_ok());
    assert!(matches!(
        kept.progress[1].report(1.0, None, None),
        Err(Error::ProgressNotIncreasing { last, next }) if last == 2.0 && next == 1.0
    ));
    assert!(kept.progress[1].report(3.0, None, None).is_ok());
    assert_eq!(sent(&rx).len(), 2);
    drop(kept.responders);
    // Answered by its return value: two `cx.progress` calls in one handler.
    let router = Router::new().request("now", |_: &mut (), params, cx| {
        let (a, b) = (cx.progress(&params), cx.progress(&params));
        let (Some(a), Some(b)) = (a, b) else {
            return Ok(false);
        };
        a.report(5.0, None, None)?;
        Ok(b.report(4.0, None, None).is_err())
    });
    let (mut e, rx) = endpoint(router, ());
    let body = br#"{"jsonrpc":"2.0","id":1,"method":"now","params":{"_meta":{"progressToken":1}}}"#;
    assert!(e.handle(body).is_ok());
    assert_eq!(
        sent(&rx).last().map(String::as_str),
        Some(r#"{"jsonrpc":"2.0","id":1,"result":true}"#)
    );
}

#[test]
fn test_stale_handles_racing_an_id_reuse_never_reach_the_new_request() {
    for round in 0..100 {
        let router = Router::new().deferred("work", |kept: &mut Kept, _, cx, responder| {
            // Marks, on the wire, the moment the request is handled.
            cx.notify("started", ()).ok();
            kept.notifiers.push(responder.notifier());
            kept.responders.push(responder);
        });
        let (e, rx) = endpoint(router, Kept::default());
        let mut e = e.with_config(Config::mcp());
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"work"}"#;
        assert!(e.handle(body).is_ok());
        let mut first = std::mem::take(e.state_mut());
        let (Some(notifier), Some(responder)) = (first.notifiers.pop(), first.responders.pop())
        else {
            panic!("no handles");
        };
        let worker = std::thread::spawn(move || {
            while notifier.notify("old", ()).is_ok() {}
            // Cancelled: the late answer must reach no one.
            responder.ok("OLD")
        });
        let cancel =
            br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1}}"#;
        assert!(e.handle(cancel).is_ok());
        assert!(e.handle(body).is_ok()); // the id, reused
        let late = worker.join();
        assert!(
            matches!(late, Ok(Err(Error::AlreadyAnswered))),
            "round {round}: {late:?}"
        );
        let second = std::mem::take(e.state_mut());
        assert!(second.responders.into_iter().all(|r| r.ok("NEW").is_ok()));
        let traffic = sent(&rx);
        let reused = traffic.iter().rposition(|m| m.contains("started"));
        let after = reused.map_or(&traffic[..], |at| &traffic[at..]);
        assert!(
            !after.iter().any(|m| m.contains("old") || m.contains("OLD")),
            "round {round}: {after:?}"
        );
        assert_eq!(
            after.last().map(String::as_str),
            Some(r#"{"jsonrpc":"2.0","id":1,"result":"NEW"}"#)
        );
    }
}
