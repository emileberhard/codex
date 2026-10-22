// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
//! V3 transcript reconciliation across interleaved speakers, reconnects, and turn boundaries.

use super::*;
use codex_protocol::protocol::RealtimeTranscriptDone;
use pretty_assertions::assert_eq;

fn events(transcript_state: RealtimeTranscriptState) -> RealtimeWebsocketEvents {
    let (_, rx_message) = async_channel::unbounded();
    RealtimeWebsocketEvents {
        rx_message,
        pending_events: Arc::new(Mutex::new(VecDeque::new())),
        transcript_state,
        event_parser: RealtimeEventParser::FramelessBidi,
        is_closed: Arc::new(AtomicBool::new(false)),
    }
}

fn entry(role: &str, text: &str) -> RealtimeTranscriptEntry {
    RealtimeTranscriptEntry {
        role: role.to_string(),
        text: text.to_string(),
    }
}

async fn observe(events: &RealtimeWebsocketEvents, mut event: RealtimeEvent) {
    let original = event.clone();
    events.update_active_transcript(&mut event).await;
    assert_eq!(
        event, original,
        "live transcript events must pass through unchanged"
    );
}

#[tokio::test]
async fn interleaved_transcripts_reconcile_across_reconnect_in_either_completion_order() {
    for assistant_finishes_first in [false, true] {
        let state = RealtimeTranscriptState::default();
        let first = events(state.clone());
        for event in [
            RealtimeEvent::InputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "What can ".into(),
            }),
            RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "A bunch ".into(),
            }),
            RealtimeEvent::InputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "you do?".into(),
            }),
            RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "of stuff".into(),
            }),
        ] {
            observe(&first, event).await;
        }
        let reconnected = events(state);
        let user = RealtimeEvent::InputTranscriptDone(RealtimeTranscriptDone {
            text: "What can you do?".into(),
        });
        let assistant = RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
            text: "A bunch of stuff, including coding.".into(),
        });
        let finals = if assistant_finishes_first {
            [assistant, user]
        } else {
            [user, assistant]
        };
        for event in finals {
            observe(&reconnected, event).await;
        }
        assert_eq!(
            reconnected.take_transcript_tail().await,
            vec![
                entry("user", "What can you do?"),
                entry("assistant", "A bunch of stuff, including coding."),
            ]
        );
        assert_eq!(reconnected.take_transcript_tail().await, vec![]);
    }
}

#[tokio::test]
async fn completed_utterances_keep_repeated_speech_and_final_only_turns_distinct() {
    let events = events(RealtimeTranscriptState::default());
    for role in ["user", "assistant"] {
        for with_delta in [true, true, false] {
            if with_delta {
                let delta = RealtimeTranscriptDelta {
                    delta: "Again".into(),
                };
                observe(
                    &events,
                    if role == "user" {
                        RealtimeEvent::InputTranscriptDelta(delta)
                    } else {
                        RealtimeEvent::OutputTranscriptDelta(delta)
                    },
                )
                .await;
            }
            let done = RealtimeTranscriptDone {
                text: "Again.".into(),
            };
            observe(
                &events,
                if role == "user" {
                    RealtimeEvent::InputTranscriptDone(done)
                } else {
                    RealtimeEvent::OutputTranscriptDone(done)
                },
            )
            .await;
            // An empty delta must not reopen the utterance we just completed.
            let empty = RealtimeTranscriptDelta {
                delta: String::new(),
            };
            observe(
                &events,
                if role == "user" {
                    RealtimeEvent::InputTranscriptDelta(empty)
                } else {
                    RealtimeEvent::OutputTranscriptDelta(empty)
                },
            )
            .await;
        }
        assert_eq!(
            events.take_transcript_tail().await,
            vec![entry(role, "Again."); 3]
        );
    }
}

#[tokio::test]
async fn provider_error_absorbs_late_finals_across_tail_drains_without_deduplicating_new_output() {
    for drain_before_error in [None, Some(false), Some(true)] {
        for late_text in [Some("Hello"), Some("a different final"), Some(""), None] {
            let first = events(RealtimeTranscriptState::default());
            observe(
                &first,
                RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                    delta: "Hello".into(),
                }),
            )
            .await;
            let mut retained = if drain_before_error == Some(true) {
                first.take_transcript_tail().await
            } else {
                Vec::new()
            };
            for _ in 0..2 {
                observe(
                    &first,
                    RealtimeEvent::ProviderError(codex_protocol::protocol::RealtimeProviderError {
                        message: "invalid request".into(),
                        code: None,
                        r#type: None,
                        param: None,
                        event_id: None,
                        client_event_id: None,
                    }),
                )
                .await;
            }
            if drain_before_error == Some(false) {
                retained.extend(first.take_transcript_tail().await);
            }
            let reconnected = events(first.transcript_state());
            if let Some(text) = late_text {
                observe(
                    &reconnected,
                    RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                        delta: String::new(),
                    }),
                )
                .await;
                observe(
                    &reconnected,
                    RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
                        text: text.into(),
                    }),
                )
                .await;
            }
            observe(
                &reconnected,
                RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                    delta: "Hello".into(),
                }),
            )
            .await;
            observe(
                &reconnected,
                RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
                    text: "Hello".into(),
                }),
            )
            .await;
            retained.extend(reconnected.take_transcript_tail().await);
            assert_eq!(retained, vec![entry("assistant", "Hello"); 2]);
        }
    }
}

#[tokio::test]
async fn provider_error_without_open_assistant_output_preserves_final_only_output() {
    let events = events(RealtimeTranscriptState::default());
    for _ in 0..2 {
        observe(
            &events,
            RealtimeEvent::ProviderError(codex_protocol::protocol::RealtimeProviderError {
                message: "invalid request".into(),
                code: None,
                r#type: None,
                param: None,
                event_id: None,
                client_event_id: None,
            }),
        )
        .await;
        observe(
            &events,
            RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
                text: "Hello".into(),
            }),
        )
        .await;
    }
    assert_eq!(
        events.take_transcript_tail().await,
        vec![entry("assistant", "Hello"); 2]
    );
}

#[tokio::test]
async fn provider_error_delta_only_policy_survives_interleaved_finals_and_handoffs() {
    for old_final in [0, 1, 2] {
        let first = events(RealtimeTranscriptState::default());
        observe(
            &first,
            RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "old".into(),
            }),
        )
        .await;
        let mut retained = first.take_transcript_tail().await;
        observe(
            &first,
            RealtimeEvent::ProviderError(codex_protocol::protocol::RealtimeProviderError {
                message: "invalid request".into(),
                code: None,
                r#type: None,
                param: None,
                event_id: None,
                client_event_id: None,
            }),
        )
        .await;
        let reconnected = events(first.transcript_state());
        if old_final == 0 {
            observe(
                &reconnected,
                RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone { text: "old".into() }),
            )
            .await;
        }
        observe(
            &reconnected,
            RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "new".into(),
            }),
        )
        .await;
        let mut handoff =
            RealtimeEvent::HandoffRequested(codex_protocol::protocol::RealtimeHandoffRequested {
                handoff_id: "handoff".into(),
                item_id: "handoff".into(),
                input_transcript: String::new(),
                active_transcript: Vec::new(),
            });
        reconnected.update_active_transcript(&mut handoff).await;
        if let RealtimeEvent::HandoffRequested(handoff) = handoff {
            retained.extend(handoff.active_transcript);
        }
        if old_final == 1 {
            observe(
                &reconnected,
                RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone { text: "old".into() }),
            )
            .await;
        }
        for text in [
            "new with final-only revision",
            "new",
            "unavailable final-only output",
        ] {
            observe(
                &reconnected,
                RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone { text: text.into() }),
            )
            .await;
        }
        observe(
            &reconnected,
            RealtimeEvent::OutputTranscriptDelta(RealtimeTranscriptDelta {
                delta: "new".into(),
            }),
        )
        .await;
        observe(
            &reconnected,
            RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
                text: String::new(),
            }),
        )
        .await;
        retained.extend(reconnected.take_transcript_tail().await);
        assert_eq!(
            retained,
            vec![
                entry("assistant", "old"),
                entry("assistant", "new"),
                entry("assistant", "new")
            ]
        );
        let fresh = events(RealtimeTranscriptState::default());
        observe(
            &fresh,
            RealtimeEvent::OutputTranscriptDone(RealtimeTranscriptDone {
                text: "fresh final-only output".into(),
            }),
        )
        .await;
        assert_eq!(
            fresh.take_transcript_tail().await,
            vec![entry("assistant", "fresh final-only output")]
        );
    }
}
