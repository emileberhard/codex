// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use pretty_assertions::assert_eq;

#[test]
fn full_identified_input_ledger_closes_absent_id_without_another_fence() {
    let mut acceptances = InputAcceptances::default();
    assert_eq!(acceptances.stop_action("absent"), InputStopAction::Fence);
    acceptances.entries.extend(
        (0..MAX_ACCEPTED_INPUTS).map(|index| (format!("input-{index}"), AcceptedInput::Rejected)),
    );
    acceptances.entries.insert(
        "input-0".to_string(),
        AcceptedInput::Accepted {
            fingerprint: "fingerprint".to_string(),
            submission: TurnInputSubmission::Started {
                turn_id: "accepted-turn".to_string(),
                root_turn_id: "accepted-turn".to_string(),
            },
        },
    );

    assert_eq!(
        [
            acceptances.stop_action("absent"),
            acceptances.stop_action("input-1"),
            acceptances.stop_action("input-0"),
        ],
        [
            InputStopAction::Unavailable,
            InputStopAction::Unavailable,
            InputStopAction::Interrupt("accepted-turn".to_string()),
        ]
    );
}
