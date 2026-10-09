use super::*;
use pretty_assertions::assert_eq;

struct MailFixture {
    harness: AgentControlHarness,
    root: Arc<CodexThread>,
    child: Arc<CodexThread>,
    control: LocalAgentControl,
}

impl MailFixture {
    async fn new() -> Self {
        let (home, mut config) = test_config().await;
        config.features.enable(Feature::MultiAgentV2).expect("V2");
        config.features.enable(Feature::Sqlite).expect("SQLite");
        config.model = Some("gpt-5.6-sol".into());
        config.multi_agent_v2.max_concurrent_threads_per_session = 2;
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
    fixture.assert_received(&twice, &[first, second]).await;
    stop(&twice).await;
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
