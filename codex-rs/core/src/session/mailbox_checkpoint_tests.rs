use super::*;
use pretty_assertions::assert_eq;

#[test_case::test_case(false; "receipt")]
#[test_case::test_case(true; "cancelled_waiter")]
#[tokio::test]
async fn partial_mail_checkpoint_real_consumption_replays_once(cancel: bool) {
    let (mut session, turn) = make_session_and_context().await;
    let rollout_path = attach_thread_persistence(&mut session).await;
    let mut first = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").expect("path"),
        vec![AgentPath::root().join("other").expect("path")],
        "accepted first".into(),
        false,
    );
    first.id = Some(codex_protocol::ResponseItemId::with_suffix(
        "amsg",
        "partial_first",
    ));
    first.encrypted_content = Some("encrypted first".into());
    let mut second = first.clone();
    second.id = Some(codex_protocol::ResponseItemId::with_suffix(
        "amsg",
        "partial_second",
    ));
    second.content = "accepted second".into();
    second.encrypted_content = Some("encrypted second".into());
    for mail in [&first, &second] {
        session
            .input_queue
            .enqueue_mailbox_communication(mail.clone(), Default::default())
            .await;
    }
    session
        .services
        .live_thread
        .as_ref()
        .expect("writer")
        .checkpoint_agent_mail(vec![first.clone()])
        .await
        .expect("persisted checkpoint prefix");
    session
        .services
        .thread_store
        .shutdown_thread(session.thread_id)
        .await
        .expect("writer failure");
    let mut admission = Some(
        session
            .services
            .local_agent_runtime
            .acquire_residency_checkpoint()
            .expect("admission"),
    );
    session
        .checkpoint_mailbox_for_eviction(&mut admission)
        .await
        .expect_err("checkpoint suffix refuses missing writer");
    let session = Arc::new(session);
    assert!(
        session
            .input_queue
            .drain_mailbox_input_items()
            .await
            .is_err()
    );
    session
        .reconcile_mailbox_before_consumption()
        .await
        .expect_err("unresolved storage remains refused");
    assert!(session.input_queue.has_unresolved_mail_checkpoint());
    let stored = session
        .services
        .thread_store
        .read_thread(codex_thread_store::ReadThreadParams {
            thread_id: session.thread_id,
            include_archived: false,
            include_history: false,
        })
        .await
        .expect("stored thread");
    let (writer, _) = LiveThread::resume(
        session.services.thread_store.clone(),
        stored.history_mode,
        codex_thread_store::ResumeThreadParams {
            thread_id: session.thread_id,
            rollout_path: stored.rollout_path,
            history: None,
            history_revision: None,
            include_archived: false,
            metadata: codex_thread_store::ThreadPersistenceMetadata {
                cwd: Some(stored.cwd),
                model_provider: stored.model_provider,
                memory_mode: codex_protocol::protocol::ThreadMemoryMode::Enabled,
            },
        },
    )
    .await
    .expect("recovered canonical writer");
    let mut fresh = first.clone();
    fresh.id = Some(codex_protocol::ResponseItemId::with_suffix(
        "amsg",
        "fresh_stamped",
    ));
    fresh.content = "fresh ordinary consumption".into();
    fresh.encrypted_content = None;
    fresh.trigger_turn = true;
    let mut later = fresh.clone();
    later.id = Some(codex_protocol::ResponseItemId::with_suffix(
        "amsg",
        "after_trigger",
    ));
    later.content = "mail after the trigger".into();
    later.trigger_turn = false;
    for mail in [&fresh, &later] {
        session
            .input_queue
            .enqueue_mailbox_communication(mail.clone(), Default::default())
            .await;
    }
    let mut recovery = Box::pin(session.reconcile_mailbox_before_consumption());
    assert!(futures::poll!(recovery.as_mut()).is_pending());
    if cancel {
        drop(recovery);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while session.input_queue.has_unresolved_mail_checkpoint() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned reconciliation settles after waiter cancellation");
    } else {
        recovery
            .await
            .expect("checkpoint settles before consumption");
    }
    let input = session
        .input_queue
        .drain_mailbox_input_items()
        .await
        .expect("mailbox input remains available")
        .0;
    assert_eq!(
        input,
        vec![
            TurnInput::InterAgentCommunication(fresh.clone()),
            TurnInput::InterAgentCommunication(later.clone())
        ]
    );
    for mail in [fresh.clone(), later.clone()] {
        session
            .record_inter_agent_communication(&turn, turn.model_info(), mail)
            .await;
    }
    let live = raw_history_items(&session.clone_history().await);
    let mut expected = vec![first.to_model_input_item(), second.to_model_input_item()];
    for (mail, recorded) in [fresh, later].iter().zip(live.iter().skip(2)) {
        let mut stamped = mail.to_model_input_item();
        stamped.set_turn_id_if_missing(&turn.sub_id);
        stamped.set_create_time_if_missing(
            recorded
                .executed_tool_call_metadata()
                .and_then(|metadata| metadata.create_time.clone())
                .expect("real consumption stamps creation time"),
        );
        expected.push(stamped);
    }
    assert_eq!(live, expected);
    session.flush_rollout().await.expect("settled consumption");
    let InitialHistory::Resumed(resumed) = RolloutRecorder::get_rollout_history(&rollout_path)
        .await
        .expect("canonical history")
    else {
        panic!("resumed history");
    };
    for _ in 0..2 {
        let (restored, context) = make_session_and_context().await;
        let history = restored
            .reconstruct_history_from_rollout(&context, &resumed.history)
            .await;
        let items = history
            .history
            .into_iter()
            .map(|envelope| envelope.item)
            .filter(|item| matches!(item, ResponseItem::AgentMessage { .. }))
            .collect::<Vec<_>>();
        assert_eq!(items, expected);
    }
    let canonical_mail: Vec<_> = resumed
        .history
        .iter()
        .filter_map(|item| match item {
            RolloutItem::InterAgentCommunication(mail) => Some(mail.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(canonical_mail, vec![first.clone(), second.clone()]);
    let agent_records: Vec<_> = resumed
        .history
        .iter()
        .filter_map(|item| match item {
            RolloutItem::InterAgentCommunication(mail) => Some(mail.to_model_input_item()),
            RolloutItem::ResponseItem(envelope)
                if matches!(envelope.item, ResponseItem::AgentMessage { .. }) =>
            {
                Some(envelope.item.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(agent_records, expected);
    println!(
        "NATIVE_MAIL_FIXTURE={}",
        serde_json::json!({
            "threadId": session.thread_id, "turnId": turn.sub_id,
            "rollout": std::fs::read_to_string(&rollout_path).expect("fixture bytes"),
            "communications": canonical_mail, "modelInput": expected,
        })
    );
    writer.shutdown().await.expect("close writer");
}
