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
    events
        .update_active_transcript(&mut event, /*frame*/ None, /*input_turn*/ None)
        .await;
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
        reconnected
            .update_active_transcript(&mut handoff, /*frame*/ None, /*input_turn*/ None)
            .await;
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

#[tokio::test]
async fn transcript_budget_uses_frame_recency_without_reordering_speakers() {
    use serde_json::json;

    let old_user = "u".repeat(4000);
    let assistant = "a".repeat(4000);
    let newest_user = "n".repeat(211);
    let current_user = "u".repeat(4160);
    let same_frame_user = "n".repeat(22);
    let unicode_user = "é".repeat(2000);
    let oversized_user = "u".repeat(MAX_ACTIVE_TRANSCRIPT_BYTES);
    let extended_assistant = format!("{assistant}{newest_user}!");
    for (frames, final_assistant, expected) in [
        (
            vec![
                ("user", old_user.as_str(), Some(0)),
                ("assistant", assistant.as_str(), Some(200)),
                ("user", newest_user.as_str(), Some(400)),
                // Commit eviction before the delayed final arrives.
                ("user", "", Some(600)),
            ],
            // A late final must keep the evicted assistant's old raw-frame age.
            Some(assistant.as_str()),
            vec![entry("user", &format!("{old_user}{newest_user}"))],
        ),
        (
            vec![
                ("assistant", assistant.as_str(), Some(0)),
                ("user", current_user.as_str(), Some(200)),
                // The first event already exceeds the budget, before output updates its age.
                ("user", same_frame_user.as_str(), Some(400)),
                // Same-frame input is emitted first; output must not become newer.
                ("assistant", "bbbbbbbbbbbbbbbbbbbb", Some(400)),
            ],
            None,
            vec![
                entry("assistant", &format!("{assistant}bbbbbbbbbbbbbbbbbbbb")),
                entry("user", &format!("…{}{same_frame_user}", "u".repeat(4128))),
            ],
        ),
        (
            vec![
                ("user", old_user.as_str(), Some(0)),
                ("assistant", assistant.as_str(), Some(200)),
                ("user", newest_user.as_str(), Some(400)),
                ("assistant", "b", Some(400)),
            ],
            None,
            vec![
                entry("user", &format!("…{}{newest_user}", "u".repeat(3958))),
                entry("assistant", &format!("{assistant}b")),
            ],
        ),
        (
            vec![
                ("user", old_user.as_str(), Some(0)),
                ("assistant", assistant.as_str(), Some(200)),
                // Reversing the same-frame events must retain the same suffixes.
                ("assistant", "b", Some(400)),
                ("user", newest_user.as_str(), Some(400)),
                // Advancing the frame also commits eviction before the tail is taken.
                ("user", "", Some(600)),
            ],
            None,
            vec![
                entry("user", &format!("…{}{newest_user}", "u".repeat(3958))),
                entry("assistant", &format!("{assistant}b")),
            ],
        ),
        (
            vec![
                ("user", unicode_user.as_str(), Some(0)),
                ("assistant", assistant.as_str(), Some(200)),
                ("user", newest_user.as_str(), Some(400)),
                ("assistant", newest_user.as_str(), Some(400)),
            ],
            None,
            vec![
                entry("user", &format!("…{}{newest_user}", "é".repeat(1937))),
                entry("assistant", &format!("…{}{newest_user}", "a".repeat(3871))),
            ],
        ),
        (
            vec![
                ("user", oversized_user.as_str(), Some(0)),
                ("assistant", "b", Some(0)),
            ],
            None,
            vec![
                entry("user", &format!("…{}", "u".repeat(8169))),
                entry("assistant", "b"),
            ],
        ),
        (
            vec![
                ("assistant", assistant.as_str(), Some(0)),
                ("user", old_user.as_str(), Some(200)),
                ("assistant", newest_user.as_str(), Some(400)),
                ("user", "b", Some(400)),
                // Trim the tied utterances before a final extends the retained suffix.
                ("user", "", Some(600)),
            ],
            Some(extended_assistant.as_str()),
            vec![
                entry("assistant", &format!("…{}{newest_user}!", "a".repeat(3957))),
                entry("user", &format!("{old_user}b")),
            ],
        ),
        (
            vec![
                ("user", old_user.as_str(), None),
                ("assistant", assistant.as_str(), None),
                ("user", newest_user.as_str(), None),
            ],
            None,
            vec![entry("assistant", &assistant)],
        ),
        (
            vec![
                ("assistant", "I will", Some(0)),
                ("user", "Stop!", Some(200)),
                ("assistant", " do it", Some(200)),
            ],
            Some("I will do it"),
            vec![entry("assistant", "I will do it"), entry("user", "Stop!")],
        ),
    ] {
        let state = RealtimeTranscriptState::default();
        let mut events = events(state.clone());
        let (sender, receiver) = async_channel::unbounded();
        events.rx_message = receiver;
        for (role, text, start_ms) in frames {
            let event_type = if role == "user" {
                "input_transcript.added"
            } else {
                "output_transcript.added"
            };
            let mut payload = json!({"type": event_type, "item": {"text": text}});
            if let Some(start_ms) = start_ms {
                payload["start_ms"] = json!(start_ms);
                payload["end_ms"] = json!(start_ms + 200);
            }
            sender
                .send(Ok(Message::Text(payload.to_string().into())))
                .await
                .unwrap();
            let delta = RealtimeTranscriptDelta { delta: text.into() };
            assert_eq!(
                events.next_event().await.unwrap(),
                Some(if role == "user" {
                    RealtimeEvent::InputTranscriptDelta(delta)
                } else {
                    RealtimeEvent::OutputTranscriptDelta(delta)
                }),
            );
        }
        if let Some(text) = final_assistant {
            sender.send(Ok(Message::Text(json!({
                "type": "turn.done",
                "turn": {"role": "assistant", "transcript": text, "start_ms": 0, "end_ms": 800}
            }).to_string().into()))).await.unwrap();
            assert_eq!(
                events.next_event().await.unwrap(),
                Some(RealtimeEvent::OutputTranscriptDone(
                    RealtimeTranscriptDone { text: text.into() }
                )),
            );
        }
        assert_eq!(state.take_transcript_tail().await, expected);
        assert_eq!(state.take_transcript_tail().await, vec![]);
    }
}

fn wire_events(
    transcript_state: RealtimeTranscriptState,
    frames: Vec<serde_json::Value>,
) -> RealtimeWebsocketEvents {
    let (tx, rx_message) = async_channel::unbounded();
    for frame in frames {
        tx.try_send(Ok(Message::Text(frame.to_string().into())))
            .unwrap();
    }
    RealtimeWebsocketEvents {
        rx_message,
        pending_events: Arc::new(Mutex::new(VecDeque::new())),
        transcript_state,
        event_parser: RealtimeEventParser::FramelessBidi,
        is_closed: Arc::new(AtomicBool::new(false)),
    }
}

#[tokio::test]
async fn handed_off_turn_final_stays_canonical_without_reopening_caller_context() {
    use serde_json::json;

    let state = RealtimeTranscriptState::default();
    let first = wire_events(
        state.clone(),
        vec![
            json!({"type": "input_transcript.added", "item": {"text": "Again."}}),
            json!({"type": "delegation.created", "item": {
                "id": "handoff-1", "type": "delegation", "target": "client",
                "user_bidi_turn_id": "turn-1", "content": [{"type": "input_text", "text": "Again."}]
            }}),
        ],
    );
    first.next_event().await.unwrap().unwrap();
    let Some(RealtimeEvent::HandoffRequested(handoff)) = first.next_event().await.unwrap() else {
        panic!("handoff");
    };
    assert_eq!(handoff.active_transcript, vec![entry("user", "Again.")]);

    let reconnected = wire_events(
        state.clone(),
        vec![
            json!({"type": "output_transcript.added", "item": {"text": "Done."}}),
            json!({"type": "turn.done", "turn": {"id": "turn-1", "role": "user", "transcript": "Again."}}),
            json!({"type": "turn.done", "turn": {"id": "turn-2", "role": "user", "transcript": "Again."}}),
            json!({"type": "delegation.created", "item": {
                "id": "handoff-2", "type": "delegation", "target": "client",
                "user_bidi_turn_id": "turn-2", "content": [{"type": "input_text", "text": "Again."}]
            }}),
        ],
    );
    reconnected.next_event().await.unwrap().unwrap();
    for _ in 0..2 {
        assert_eq!(
            reconnected.next_event().await.unwrap(),
            Some(RealtimeEvent::InputTranscriptDone(RealtimeTranscriptDone {
                text: "Again.".into()
            }))
        );
    }
    assert!(state.active.lock().await.forwarded_input_turns.is_empty());
    let Some(RealtimeEvent::HandoffRequested(handoff)) = reconnected.next_event().await.unwrap()
    else {
        panic!("handoff");
    };
    assert_eq!(
        handoff.active_transcript,
        vec![entry("assistant", "Done."), entry("user", "Again.")]
    );
}

#[tokio::test]
async fn missing_finals_evict_oldest_identity_without_ending_the_stream() {
    use serde_json::json;

    let oldest = "a".repeat(4096);
    let newest = "b".repeat(4096);
    let state = RealtimeTranscriptState::default();
    let stream = wire_events(
        state.clone(),
        vec![
            json!({"type": "delegation.created", "item": {
                "id": "handoff-1", "type": "delegation", "target": "client",
                "user_bidi_turn_id": oldest, "content": [{"type": "input_text", "text": "First."}]
            }}),
            json!({"type": "delegation.created", "item": {
                "id": "handoff-2", "type": "delegation", "target": "client",
                "user_bidi_turn_id": newest, "content": [{"type": "input_text", "text": "Second."}]
            }}),
            json!({"type": "turn.done", "turn": {"id": newest, "role": "user", "transcript": "Second."}}),
            json!({"type": "turn.done", "turn": {"id": oldest, "role": "user", "transcript": "First."}}),
            json!({"type": "session.closed", "reason": "requested"}),
        ],
    );
    for _ in 0..2 {
        assert!(matches!(
            stream.next_event().await.unwrap(),
            Some(RealtimeEvent::HandoffRequested(_))
        ));
    }
    {
        let active = state.active.lock().await;
        assert_eq!(active.forwarded_input_turns, VecDeque::from([newest]));
        assert!(active.forwarded_input_overflow_reported);
    }
    stream.next_event().await.unwrap().unwrap();
    assert!(state.active.lock().await.forwarded_input_turns.is_empty());
    assert_eq!(stream.take_transcript_tail().await, vec![]);
    stream.next_event().await.unwrap().unwrap();
    assert_eq!(
        stream.take_transcript_tail().await,
        vec![entry("user", "First.")]
    );
    assert_eq!(
        stream.next_event().await.unwrap(),
        Some(RealtimeEvent::SessionClosed {
            reason: Some("requested".into())
        })
    );
}
