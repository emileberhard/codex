// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use codex_protocol::items::AgentMessageContent;
use codex_protocol::items::AgentMessageItem;
use codex_protocol::items::ReasoningItem;
use codex_protocol::items::TurnItem;
use codex_protocol::protocol::AgentMessageContentDeltaEvent;
use codex_protocol::protocol::ItemCompletedEvent;
use codex_protocol::protocol::ItemStartedEvent;
use codex_protocol::protocol::ReasoningContentDeltaEvent;
use pretty_assertions::assert_eq;

fn message(text: &str) -> TurnItem {
    TurnItem::AgentMessage(AgentMessageItem {
        id: "message".into(),
        content: vec![AgentMessageContent::Text { text: text.into() }],
        phase: None,
        memory_citation: None,
        delivery: None,
        questions: None,
    })
}

fn start(item: TurnItem) -> EventMsg {
    EventMsg::ItemStarted(ItemStartedEvent {
        thread_id: codex_protocol::ThreadId::default(),
        turn_id: "turn".into(),
        item,
        started_at_ms: 1000,
    })
}

fn delta(text: &str) -> EventMsg {
    EventMsg::AgentMessageContentDelta(AgentMessageContentDeltaEvent {
        thread_id: "thread".into(),
        turn_id: "turn".into(),
        item_id: "message".into(),
        delta: text.into(),
    })
}

fn complete(item: TurnItem) -> EventMsg {
    EventMsg::ItemCompleted(ItemCompletedEvent {
        thread_id: codex_protocol::ThreadId::default(),
        turn_id: "turn".into(),
        item,
        started_at_ms: Some(1000),
        completed_at_ms: 2000,
    })
}

#[test]
fn checkpoint_contains_consumed_message_prefix_and_completion_releases_it() {
    let mut state = ObservationItems::default();
    state.track("turn", &start(message("")));
    state.track("turn", &delta("before "));
    let checkpoint = state.snapshot().unwrap();
    state.track("turn", &delta("after"));
    assert_eq!(
        checkpoint,
        vec![ObservedItem {
            turn_id: "turn".into(),
            item: ThreadItem::from(message("before ")),
            started_at_ms: Some(1000),
            completed_at_ms: None,
            is_complete: false,
        }]
    );
    assert_eq!(
        state.snapshot().unwrap()[0].item,
        ThreadItem::from(message("before after"))
    );
    state.track("turn", &complete(message("before after")));
    assert!(state.snapshot().unwrap().is_empty());
}

#[test]
fn checkpoint_preserves_reasoning_section_indices() {
    let mut state = ObservationItems::default();
    let reasoning = TurnItem::Reasoning(ReasoningItem {
        id: "reasoning".into(),
        summary_text: vec![],
        raw_content: vec![],
    });
    state.track("turn", &start(reasoning));
    for (index, delta) in [(0, "first "), (0, "section"), (1, "second")] {
        state.track(
            "turn",
            &EventMsg::ReasoningContentDelta(ReasoningContentDeltaEvent {
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item_id: "reasoning".into(),
                delta: delta.into(),
                summary_index: index,
            }),
        );
    }
    assert_eq!(
        state.snapshot().unwrap()[0].item,
        ThreadItem::Reasoning {
            id: "reasoning".into(),
            summary: vec!["first section".into(), "second".into()],
            content: vec![],
        }
    );
}

#[test]
fn oversized_prefix_fails_observation_without_retaining_truncated_text() {
    let mut state = ObservationItems::default();
    state.track("turn", &start(message("")));
    state.track("turn", &delta(&"a".repeat(MAX_UNFINISHED_BYTES + 1)));
    assert!(state.snapshot().is_err());
    assert_eq!(state.bytes, 0);
    state.track("turn", &complete(message("complete")));
    assert!(state.snapshot().unwrap().is_empty());
}

#[test]
fn delta_without_its_start_never_claims_a_complete_prefix() {
    let mut state = ObservationItems::default();
    state.track("turn", &delta("missing prefix"));
    assert!(state.snapshot().is_err());
}

#[test]
fn checkpoint_preserves_listener_start_order_without_durable_positions() {
    let mut state = ObservationItems::default();
    for id in ["z", "a"] {
        let TurnItem::AgentMessage(mut item) = message("") else {
            unreachable!()
        };
        item.id = id.into();
        state.track("turn", &start(TurnItem::AgentMessage(item)));
    }
    assert_eq!(
        state
            .snapshot()
            .unwrap()
            .iter()
            .map(|entry| entry.item.id())
            .collect::<Vec<_>>(),
        vec!["z", "a"]
    );
}

#[test]
fn ephemeral_checkpoint_retains_completion_and_reports_eviction() {
    let mut state = ObservationItems::new(true);
    state.track("turn", &start(message("")));
    state.track("turn", &complete(message("answer")));
    let snapshot = state.snapshot().unwrap();
    assert_eq!(snapshot[0].item, ThreadItem::from(message("answer")));
    assert!(snapshot[0].is_complete);
    assert_eq!(snapshot[0].completed_at_ms, Some(2000));
    for index in 0..MAX_UNFINISHED_ITEMS {
        let TurnItem::AgentMessage(mut item) = message("next") else {
            unreachable!()
        };
        item.id = format!("message-{index}");
        state.track("turn", &complete(TurnItem::AgentMessage(item)));
    }
    assert!(state.history_unavailable);
    assert_eq!(state.snapshot().unwrap().len(), MAX_UNFINISHED_ITEMS);
    assert!(
        state
            .snapshot()
            .unwrap()
            .iter()
            .all(|entry| entry.item.id() != "message")
    );
}
