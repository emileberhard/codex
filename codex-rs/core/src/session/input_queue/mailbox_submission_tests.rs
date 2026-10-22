// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use crate::session::tests::make_session_and_context;
use codex_protocol::protocol::TurnAbortReason;
use codex_protocol::turn_input::TurnInputMode;
use codex_protocol::turn_input::TurnInputRequest;
use pretty_assertions::assert_eq;

#[tokio::test]
async fn submission_refuses_checkpoint_failure_while_mailbox_drain_waits() {
    let (session, _) = make_session_and_context().await;
    let session = Arc::new(session);
    let communication = InterAgentCommunication::new(
        codex_protocol::AgentPath::root(),
        codex_protocol::AgentPath::root(),
        Vec::new(),
        "accepted mail before the explicit turn".into(),
        false,
    );
    session
        .input_queue
        .enqueue_mailbox_communication(communication, Default::default())
        .await;
    let mailbox = session.input_queue.mailbox_pending_mails.lock().await;
    let mut admission = Some(
        session
            .services
            .local_agent_runtime
            .acquire_residency_checkpoint()
            .expect("admission"),
    );
    let mut checkpoint = Box::pin(session.checkpoint_mailbox_for_eviction(&mut admission));
    assert!(futures::poll!(checkpoint.as_mut()).is_pending());
    let mut submission = Box::pin(crate::session::turn_input::handle(
        &session,
        TurnInputRequest::user_input(vec![UserInput::Text {
            text: "explicit concurrent input".into(),
            text_elements: Vec::new(),
        }]),
        TurnInputMode::StartOrSteer,
        "concurrent-mailbox-submission".into(),
    ));
    assert!(futures::poll!(submission.as_mut()).is_pending());
    assert!(!session.input_queue.has_unresolved_mail_checkpoint());
    drop(mailbox);
    checkpoint
        .await
        .expect_err("native owner refuses missing persistence");
    let result = submission.await;
    let no_reservation = session.active_turn.lock().await.is_none();
    session.abort_all_tasks(TurnAbortReason::Interrupted).await;
    assert!(
        result.is_err(),
        "concurrent failure was treated as empty mailbox: {result:?}"
    );
    assert!(no_reservation);
    assert!(session.input_queue.has_pending_mailbox_items().await);
    let active = ActiveTurn::default();
    let steer = TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: "already accepted steer".into(),
            text_elements: Vec::new(),
        }],
        client_id: None,
        metadata: Default::default(),
    };
    session
        .input_queue
        .extend_pending_input_for_turn_state(active.turn_state.as_ref(), vec![steer.clone()])
        .await;
    let active = Mutex::new(Some(active));
    session
        .input_queue
        .get_pending_input(&active)
        .await
        .expect_err("a failed mail drain cannot remove accepted steers");
    let active = active.into_inner().expect("active turn");
    session
        .input_queue
        .take_restart_input(&active)
        .await
        .expect_err("restart drain is also visibly refused");
    assert_eq!(
        session
            .input_queue
            .take_pending_input_for_turn_state(active.turn_state.as_ref())
            .await,
        vec![steer]
    );
}
