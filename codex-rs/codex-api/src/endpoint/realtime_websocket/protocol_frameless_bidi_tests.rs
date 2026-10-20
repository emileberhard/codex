use super::parse_frameless_bidi_event;
use crate::endpoint::realtime_websocket::protocol_v1::parse_realtime_event_v1;
use codex_protocol::protocol::RealtimeEvent;
use codex_protocol::protocol::RealtimeHandoffRequested;

#[test]
fn legacy_and_frameless_delegations_decode_to_the_same_handoff() {
    let expected = Some(RealtimeEvent::HandoffRequested(RealtimeHandoffRequested {
        handoff_id: "handoff-123".to_string(),
        item_id: "handoff-123".to_string(),
        input_transcript: "check the weather".to_string(),
        active_transcript: Vec::new(),
    }));
    let legacy = r#"{
        "type": "conversation.handoff.requested",
        "handoff_id": "handoff-123",
        "item_id": "handoff-123",
        "input_transcript": "check the weather"
    }"#;
    let frameless = r#"{
        "type": "delegation.created",
        "offset_ms": 1000,
        "item": {
            "id": "handoff-123",
            "type": "delegation",
            "target": "client",
            "content": [{"type": "input_text", "text": "check the weather"}]
        }
    }"#;

    assert_eq!(parse_realtime_event_v1(legacy), expected);
    assert_eq!(parse_frameless_bidi_event(frameless), expected);
}

#[test]
fn frameless_transcript_and_audio_events_reuse_existing_internal_events() {
    let input = r#"{
        "type": "input_transcript.added",
        "item": {"id": "input-1", "type": "input_transcript", "text": "hello"}
    }"#;
    let done = r#"{
        "type": "turn.done",
        "turn": {"id": "turn-1", "role": "user", "transcript": "hello"}
    }"#;
    let audio = r#"{
        "type": "output_audio.delta",
        "audio": "AAE=",
        "start_ms": 0,
        "end_ms": 100
    }"#;

    assert!(matches!(
        parse_frameless_bidi_event(input),
        Some(RealtimeEvent::InputTranscriptDelta(_))
    ));
    assert!(matches!(
        parse_frameless_bidi_event(done),
        Some(RealtimeEvent::InputTranscriptDone(_))
    ));
    assert!(matches!(
        parse_frameless_bidi_event(audio),
        Some(RealtimeEvent::AudioOut(_))
    ));
}

#[test]
fn provider_errors_preserve_details_without_changing_legacy_errors() {
    use crate::endpoint::realtime_websocket::protocol_v2::parse_realtime_event_v2;
    use codex_protocol::protocol::RealtimeProviderError;
    use serde_json::json;

    for payload in [
        json!({"type": "error", "message": "original message", "code": "future_code", "param": "input", "event_id": "provider-1", "client_event_id": "client-1"}),
        json!({"type": "error", "event_id": "provider-1", "error": {"message": "original message", "code": "future_code", "param": "input", "client_event_id": "client-1"}}),
    ] {
        assert_eq!(
            parse_frameless_bidi_event(&payload.to_string()),
            Some(RealtimeEvent::ProviderError(RealtimeProviderError {
                message: "original message".to_string(),
                code: Some("future_code".to_string()),
                r#type: None,
                param: Some("input".to_string()),
                event_id: Some("provider-1".to_string()),
                client_event_id: Some("client-1".to_string()),
            }))
        );
        for parser in [parse_realtime_event_v1, parse_realtime_event_v2] {
            assert_eq!(
                parser(&payload.to_string()),
                Some(RealtimeEvent::Error("original message".to_string()))
            );
        }
    }
    let payload = json!({"type": "error", "client_event_id": "outer", "error": {"message": "unchanged", "type": "invalid_request_error", "code": null}});
    let Some(RealtimeEvent::ProviderError(error)) =
        parse_frameless_bidi_event(&payload.to_string())
    else {
        panic!("provider error");
    };
    assert_eq!(error.r#type.as_deref(), Some("invalid_request_error"));
    assert_eq!(error.client_event_id.as_deref(), Some("outer"));
    assert_eq!(error.code, None);
}

#[test]
fn provider_close_preserves_known_unknown_and_missing_reasons() {
    for reason in [
        Some("close_requested"),
        Some("expired"),
        Some("content"),
        Some("remote_hangup"),
        Some("connection_lost"),
        Some("future_reason"),
        Some("requested"),
        Some("transport_closed"),
        Some("error"),
        None,
    ] {
        let payload = serde_json::json!({"type": "session.closed", "reason": reason});
        assert_eq!(
            parse_frameless_bidi_event(&payload.to_string()),
            Some(RealtimeEvent::SessionClosed {
                reason: reason.map(str::to_owned)
            })
        );
    }
    assert_eq!(
        parse_frameless_bidi_event(r#"{"type":"session.closed"}"#),
        Some(RealtimeEvent::SessionClosed { reason: None })
    );
}
