use super::*;
use pretty_assertions::assert_eq;

#[path = "mailbox_fixture_export.rs"]
mod fixture_export;

struct MailFixture {
    harness: AgentControlHarness,
    root: Arc<CodexThread>,
    child: Arc<CodexThread>,
    control: LocalAgentControl,
}

impl MailFixture {
    async fn new() -> Self {
        Self::with_responses_url(/*responses_url*/ None).await
    }

    async fn with_responses_url(responses_url: Option<String>) -> Self {
        let (home, mut config) = test_config().await;
        config.features.enable(Feature::MultiAgentV2).expect("V2");
        config.features.enable(Feature::Sqlite).expect("SQLite");
        config.model = Some("gpt-5.6-sol".into());
        config.multi_agent_v2.max_concurrent_threads_per_session = 2;
        if let Some(responses_url) = responses_url {
            config.model_provider.base_url = Some(responses_url);
            config.model_provider.supports_websockets = false;
            config.model_provider.request_max_retries = Some(0);
        }
        let harness = AgentControlHarness::new_with_config(home, config).await;
        let (_, root) = harness.start_paginated_thread().await;
        persist_thread_for_tree_resume(&root, "root fixture").await;
        let control = root
            .session
            .services
            .local_agent_runtime
            .control(root.session.session_id());
        let child_id =
            spawn_v2_reload_test_child(&control, harness.config.clone(), &root, "worker")
                .await
                .thread_id;
        let child = harness.manager.get_thread(child_id).await.expect("child");
        persist_thread_for_tree_resume(&child, "child fixture").await;
        Self {
            harness,
            root,
            child,
            control,
        }
    }

    fn child_id(&self) -> ThreadId {
        self.child.session.thread_id
    }

    async fn evict(&self) {
        let next = spawn_v2_reload_test_child(
            &self.control,
            self.harness.config.clone(),
            &self.root,
            "next",
        )
        .await;
        assert_thread_not_loaded(&self.harness.manager, self.child_id()).await;
        let next = self
            .harness
            .manager
            .get_thread(next.thread_id)
            .await
            .expect("next child");
        persist_thread_for_tree_resume(&next, "next fixture").await;
    }

    async fn send(&self, communication: InterAgentCommunication) -> CodexResult<String> {
        self.control
            .send_inter_agent_communication(
                self.child_id(),
                communication,
                AgentCommunicationContext::new(
                    AgentCommunicationKind::Message,
                    self.root.session.thread_id,
                ),
                TurnStartOptions::default(),
            )
            .await
    }

    async fn queued(&self) {
        timeout(Duration::from_secs(5), async {
            while !self
                .child
                .session
                .input_queue
                .has_pending_mailbox_items()
                .await
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("queued mail dispatched");
    }

    async fn stored_mail(&self) -> Vec<InterAgentCommunication> {
        self.child
            .session
            .services
            .thread_store
            .load_latest_model_context(codex_thread_store::LoadThreadHistoryParams {
                thread_id: self.child_id(),
                include_archived: false,
            })
            .await
            .expect("canonical model context")
            .items
            .iter()
            .filter_map(|item| match item {
                RolloutItem::InterAgentCommunication(mail) => Some(mail.clone()),
                _ => None,
            })
            .collect()
    }

    async fn resume(&self) -> ThreadManager {
        let manager = ThreadManager::with_models_provider_home_and_state_for_tests(
            CodexAuth::from_api_key("dummy"),
            self.harness.config.model_provider.clone(),
            self.harness.config.codex_home.to_path_buf(),
            Arc::new(codex_exec_server::EnvironmentManager::default_for_tests()),
            self.harness.state_db.clone(),
        );
        manager
            .agent_control()
            .resume_agent_from_rollout(
                self.harness.config.clone(),
                self.root.session.thread_id,
                SessionSource::Exec,
            )
            .await
            .expect("resume exact root");
        assert_thread_not_loaded(&manager, self.child_id()).await;
        manager
            .ensure_multi_agent_v2_child_loaded(self.child_id())
            .await
            .expect("lazy child resume");
        manager
    }

    async fn assert_received(&self, manager: &ThreadManager, expected: &[InterAgentCommunication]) {
        let restored = manager
            .get_thread(self.child_id())
            .await
            .expect("restored child");
        let mail: Vec<_> = restored
            .session
            .clone_history()
            .await
            .raw_items()
            .filter(|item| matches!(item, ResponseItem::AgentMessage { .. }))
            .cloned()
            .collect();
        let expected: Vec<_> = expected
            .iter()
            .map(InterAgentCommunication::to_model_input_item)
            .collect();
        assert_eq!(
            mail, expected,
            "acknowledged native mail must survive a fresh runtime exactly once"
        );
        assert!(
            restored
                .session
                .input_queue
                .drain_mailbox()
                .await
                .expect("mailbox input remains available")
                .is_empty(),
            "replayed mail must not be queued again"
        );
    }

    async fn open_writer(&self) -> codex_thread_store::LiveThread {
        let store = self.child.session.services.thread_store.clone();
        let stored = store
            .read_thread(codex_thread_store::ReadThreadParams {
                thread_id: self.child_id(),
                include_archived: false,
                include_history: false,
            })
            .await
            .expect("recipient metadata");
        codex_thread_store::LiveThread::resume(
            store,
            stored.history_mode,
            codex_thread_store::ResumeThreadParams {
                thread_id: self.child_id(),
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
        .expect("exclusive native writer")
        .0
    }
}

fn message(suffix: &str) -> InterAgentCommunication {
    let mut mail = InterAgentCommunication::new(
        AgentPath::root(),
        AgentPath::root().join("worker").expect("path"),
        vec![AgentPath::root().join("next").expect("other recipient")],
        format!("accepted {suffix}"),
        false,
    );
    mail.id = Some(ResponseItemId::with_suffix("amsg", suffix));
    mail
}

async fn stop(manager: &ThreadManager) {
    manager.begin_new_work_fence();
    timeout(Duration::from_secs(5), async {
        while !manager.poll_new_work_fence() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settled process fence");
    let stopped = manager
        .shutdown_all_threads_bounded(Duration::from_secs(5))
        .await;
    assert!(stopped.submit_failed.is_empty() && stopped.timed_out.is_empty());
}

#[test_case::test_case(false; "direct_evicted_delivery")]
#[test_case::test_case(true; "unread_residency_transfer")]
#[tokio::test]
async fn v2_mail_survives_native_restart(transferred: bool) {
    let fixture = MailFixture::new().await;
    let first = message("before_eviction");
    if transferred {
        fixture
            .send(first.clone())
            .await
            .expect("loaded QueueOnly acknowledgement");
        fixture.queued().await;
    }
    fixture.evict().await;
    if !transferred {
        fixture
            .send(first.clone())
            .await
            .expect("cold QueueOnly acknowledgement");
    }
    let mut second = message("after_eviction");
    second.encrypted_content = Some("opaque native payload".into());
    fixture
        .send(second.clone())
        .await
        .expect("ordered cold mail");
    assert_thread_not_loaded(&fixture.harness.manager, fixture.child_id()).await;
    assert_eq!(
        fixture.stored_mail().await,
        vec![first.clone(), second.clone()],
        "canonical receipt preserves identity, content and order"
    );
    stop(&fixture.harness.manager).await;
    let resumed = fixture.resume().await;
    fixture
        .assert_received(&resumed, &[first.clone(), second.clone()])
        .await;
    stop(&resumed).await;
    let twice = fixture.resume().await;
    fixture
        .assert_received(&twice, &[first.clone(), second.clone()])
        .await;
    stop(&twice).await;
    if std::env::var("RC_EXPORT_NATIVE_MAIL_FIXTURES").is_ok_and(|value| value == "1") {
        fixture
            .export_fixture(
                if transferred {
                    "unread_eviction"
                } else {
                    "direct_cold"
                },
                &[first, second],
            )
            .await;
    }
}

#[tokio::test]
async fn v2_api_queue_only_preserves_lazy_recipient() {
    let fixture = MailFixture::new().await;
    fixture.evict().await;
    let receipt = fixture
        .control
        .send(crate::SendRequest {
            caller: fixture.root.session.thread_id,
            target: crate::AgentTarget::Id(fixture.child_id()),
            resume_config: fixture.harness.config.clone(),
            input: AgentInput::Message {
                message: AgentMessage::Plaintext("native API caller".into()),
                mode: MessageDeliveryMode::QueueOnly,
            },
            start_options: TurnStartOptions::default(),
        })
        .await
        .expect("API acknowledgement");
    assert!(!receipt.submission_id.is_empty());
    assert_thread_not_loaded(&fixture.harness.manager, fixture.child_id()).await;
    let mail = fixture.stored_mail().await;
    assert_eq!(mail.len(), 1);
    assert_eq!(
        (
            &mail[0].author,
            &mail[0].recipient,
            mail[0].content.as_str(),
            mail[0].trigger_turn
        ),
        (
            &AgentPath::root(),
            &AgentPath::root().join("worker").expect("path"),
            "Message Type: MESSAGE\nTask name: /root/worker\nSender: /root\nPayload:\nnative API caller",
            false
        )
    );
    assert!(mail[0].id.is_some());
    stop(&fixture.harness.manager).await;
    let resumed = fixture.resume().await;
    fixture.assert_received(&resumed, &mail).await;
    stop(&resumed).await;
}

#[test_case::test_case(false; "wait_for_receipt")]
#[test_case::test_case(true; "caller_cancellation")]
#[tokio::test]
async fn v2_cold_mail_settles_before_restart_readiness(cancel: bool) {
    let fixture = MailFixture::new().await;
    fixture.evict().await;
    let communication = message("racing_fence");
    let mut delivery = Box::pin(fixture.send(communication.clone()));
    assert!(futures::poll!(delivery.as_mut()).is_pending());
    fixture.harness.manager.begin_new_work_fence();
    assert!(!fixture.harness.manager.poll_new_work_fence());
    if cancel {
        drop(delivery);
    } else {
        delivery.await.expect("admitted checkpoint completes");
    }
    timeout(Duration::from_secs(5), async {
        while !fixture.harness.manager.poll_new_work_fence() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("checkpoint settles");
    fixture
        .send(message("after_fence"))
        .await
        .expect_err("post-fence cold delivery is refused");
    assert_eq!(fixture.stored_mail().await, vec![communication.clone()]);
    stop(&fixture.harness.manager).await;
    let resumed = fixture.resume().await;
    fixture.assert_received(&resumed, &[communication]).await;
    stop(&resumed).await;
}

#[tokio::test]
async fn v2_cold_mail_refuses_contended_writer_without_acknowledging() {
    let fixture = MailFixture::new().await;
    fixture.evict().await;
    let writer = fixture.open_writer().await;
    let refusal = fixture
        .send(message("refused"))
        .await
        .expect_err("exclusive writer refuses cold acknowledgement");
    assert!(
        refusal
            .to_string()
            .contains("already has a live local writer")
    );
    writer.shutdown().await.expect("release recipient writer");
    assert!(fixture.stored_mail().await.is_empty());
    let accepted = message("accepted_after_refusal");
    fixture
        .send(accepted.clone())
        .await
        .expect("retry after writer release");
    assert_eq!(fixture.stored_mail().await, vec![accepted]);
    stop(&fixture.harness.manager).await;
}

#[tokio::test]
async fn v2_failed_eviction_keeps_mail_and_refuses_restart_readiness() {
    let fixture = MailFixture::new().await;
    let communication = message("failed_checkpoint");
    fixture
        .send(communication.clone())
        .await
        .expect("loaded acknowledgement");
    fixture.queued().await;
    fixture
        .child
        .session
        .services
        .thread_store
        .shutdown_thread(fixture.child_id())
        .await
        .expect("storage fault");
    fixture
        .control
        .spawn(SpawnRequest {
            caller: fixture.root.session.thread_id,
            config: fixture.harness.config.clone(),
            input: AgentInput::UserInput(text_input("replacement")),
            source: thread_spawn_source(
                fixture.root.session.thread_id,
                &fixture.root.session_source,
                1,
                None,
                Some("next".into()),
            )
            .expect("source"),
            options: SpawnAgentOptions {
                parent_thread_id: Some(fixture.root.session.thread_id),
                ..Default::default()
            },
        })
        .await
        .expect_err("failed checkpoint refuses eviction");
    assert!(
        fixture
            .harness
            .manager
            .get_thread(fixture.child_id())
            .await
            .is_ok()
    );
    assert!(
        fixture
            .child
            .session
            .input_queue
            .has_pending_mailbox_items()
            .await
    );
    fixture.harness.manager.begin_new_work_fence();
    assert!(
        !fixture.harness.manager.poll_new_work_fence(),
        "unread accepted mail blocks a false restart-ready receipt"
    );
    let _writer = fixture.open_writer().await;
    fixture.harness.manager.unfence_new_work();
    fixture.evict().await;
    assert_eq!(fixture.stored_mail().await, vec![communication.clone()]);
    stop(&fixture.harness.manager).await;
    let resumed = fixture.resume().await;
    fixture.assert_received(&resumed, &[communication]).await;
    stop(&resumed).await;
}

#[tokio::test]
async fn v2_cold_checkpoint_retries_keep_full_native_identity() {
    let fixture = MailFixture::new().await;
    fixture.evict().await;
    let mut communication = message("retry_identity");
    communication.encrypted_content = Some("opaque native payload".into());
    fixture
        .send(communication.clone())
        .await
        .expect("first checkpoint");
    fixture
        .send(communication.clone())
        .await
        .expect("idempotent checkpoint retry");
    let mut different_distribution = communication.clone();
    different_distribution.other_recipients.clear();
    fixture
        .send(different_distribution)
        .await
        .expect_err("native distribution cannot change under the same identity");
    let mut different_plaintext = communication.clone();
    different_plaintext.content = "different plaintext alongside the same encrypted payload".into();
    fixture
        .send(different_plaintext)
        .await
        .expect_err("the complete envelope, not just model-visible ciphertext, owns identity");
    assert_eq!(fixture.stored_mail().await, vec![communication.clone()]);
    stop(&fixture.harness.manager).await;
    let resumed = fixture.resume().await;
    fixture.assert_received(&resumed, &[communication]).await;
    stop(&resumed).await;
}

#[tokio::test]
async fn v2_loaded_mail_refuses_after_process_fence() {
    let fixture = MailFixture::new().await;
    fixture.harness.manager.begin_new_work_fence();
    assert!(fixture.harness.manager.poll_new_work_fence());
    let refusal = fixture
        .send(message("loaded_after_fence"))
        .await
        .expect_err("post-fence loaded delivery is refused");
    assert!(refusal.to_string().contains("draining for restart"));
    assert!(
        !fixture
            .child
            .session
            .input_queue
            .has_pending_mailbox_items()
            .await
    );
    stop(&fixture.harness.manager).await;
}

#[test_case::test_case(false; "start_or_steer")]
#[test_case::test_case(true; "start_if_idle")]
#[tokio::test]
async fn v2_turn_submission_refuses_unresolved_mail_checkpoint(idle_only: bool) {
    let server = wiremock::MockServer::start().await;
    let fixture = MailFixture::with_responses_url(Some(server.uri())).await;
    let mail = message("submission_refusal");
    fixture
        .send(mail.clone())
        .await
        .expect("loaded mail receipt");
    fixture.queued().await;
    fixture
        .child
        .session
        .services
        .thread_store
        .shutdown_thread(fixture.child_id())
        .await
        .expect("writer fault");
    let mut admission = Some(
        fixture
            .child
            .session
            .services
            .local_agent_runtime
            .acquire_residency_checkpoint()
            .expect("admission"),
    );
    fixture
        .child
        .session
        .checkpoint_mailbox_for_eviction(&mut admission)
        .await
        .expect_err("failed native checkpoint");
    let request = codex_protocol::turn_input::TurnInputRequest::user_input(text_input(
        "explicit input must not disappear",
    ));
    let refused = if idle_only {
        fixture
            .child
            .start_turn_if_idle(request)
            .await
            .map(|result| format!("{result:?}"))
    } else {
        fixture
            .child
            .start_or_steer_turn(request)
            .await
            .map(|result| format!("{result:?}"))
    };
    let no_reservation = fixture.child.session.active_turn.lock().await.is_none();
    fixture
        .child
        .session
        .abort_all_tasks(TurnAbortReason::Interrupted)
        .await;
    let writer = fixture.open_writer().await;
    fixture
        .child
        .session
        .reconcile_mailbox_before_consumption()
        .await
        .expect("repair pending mail");
    let response = core_test_support::responses::mount_sse_once(
        &server,
        core_test_support::responses::sse(vec![
            core_test_support::responses::ev_response_created("mail-retry"),
            core_test_support::responses::ev_completed("mail-retry"),
        ]),
    )
    .await;
    let retry_request = codex_protocol::turn_input::TurnInputRequest::user_input(text_input(
        "explicit input must not disappear",
    ));
    let turn_id = if idle_only {
        match fixture
            .child
            .start_turn_if_idle(retry_request)
            .await
            .expect("retry accepted")
        {
            crate::StartIfIdleSubmission::Started { turn_id, .. } => turn_id,
            other => panic!("retry was refused: {other:?}"),
        }
    } else {
        match fixture
            .child
            .start_or_steer_turn(retry_request)
            .await
            .expect("retry accepted")
        {
            codex_protocol::turn_input::TurnInputSubmission::Started { turn_id, .. } => turn_id,
            other => panic!("retry was refused: {other:?}"),
        }
    };
    timeout(Duration::from_secs(5), async {
        loop {
            let event = fixture.child.next_event().await.expect("retry lifecycle");
            if matches!(event.msg, EventMsg::TurnComplete(ref completed) if completed.turn_id == turn_id) {
                break;
            }
        }
    }).await.expect("accepted retry ends through the actual task lifecycle");
    let inputs = response.single_request().input();
    let native: Vec<_> = inputs
        .iter()
        .filter(|item| item["type"] == "agent_message")
        .cloned()
        .collect();
    assert_eq!(
        native,
        vec![serde_json::to_value(mail.to_model_input_item()).expect("native mail")]
    );
    let explicit_count = inputs
        .iter()
        .filter(|item| item["role"] == "user")
        .flat_map(|item| item["content"].as_array().into_iter().flatten())
        .filter(|item| item["text"] == "explicit input must not disappear")
        .count();
    assert_eq!(explicit_count, 1);
    writer.shutdown().await.expect("close repaired writer");
    stop(&fixture.harness.manager).await;
    assert!(
        refused.is_err(),
        "unresolved persistence falsely accepted explicit input: {refused:?}"
    );
    assert!(
        no_reservation,
        "refused input retained a task-less idle reservation"
    );
}

#[tokio::test]
async fn v2_turn_submission_retains_residency_after_waiter_cancellation() {
    let fixture = MailFixture::new().await;
    fixture
        .send(message("cancelled_submission"))
        .await
        .expect("loaded receipt");
    fixture.queued().await;
    fixture
        .child
        .session
        .services
        .thread_store
        .shutdown_thread(fixture.child_id())
        .await
        .expect("storage fault");
    let mut admission = Some(
        fixture
            .child
            .session
            .services
            .local_agent_runtime
            .acquire_residency_checkpoint()
            .expect("admission"),
    );
    fixture
        .child
        .session
        .checkpoint_mailbox_for_eviction(&mut admission)
        .await
        .expect_err("pending checkpoint");
    let mut submission = Box::pin(fixture.child.start_or_steer_turn(
        codex_protocol::turn_input::TurnInputRequest::user_input(text_input("cancelled waiter")),
    ));
    assert!(futures::poll!(submission.as_mut()).is_pending());
    assert!(
        Arc::clone(&fixture.child.residency_gate)
            .try_write_owned()
            .is_err()
    );
    drop(submission);
    assert!(
        Arc::clone(&fixture.child.residency_gate)
            .try_write_owned()
            .is_err()
    );
    timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(guard) = Arc::clone(&fixture.child.residency_gate).try_write_owned() {
                drop(guard);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queued refusal settles residency ownership");
    assert!(fixture.child.session.active_turn.lock().await.is_none());
    let writer = fixture.open_writer().await;
    fixture
        .child
        .session
        .reconcile_mailbox_before_consumption()
        .await
        .expect("repair retained mail");
    writer.shutdown().await.expect("close writer");
    stop(&fixture.harness.manager).await;
}
