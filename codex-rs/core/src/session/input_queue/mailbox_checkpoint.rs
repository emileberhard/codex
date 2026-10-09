use super::InputQueue;
use crate::session::Session;
use codex_protocol::protocol::InterAgentCommunication;

impl InputQueue {
    pub(crate) fn has_unresolved_mail_checkpoint(&self) -> bool {
        self.mailbox_restart_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    fn finish_mail_checkpoint(&self) {
        self.mailbox_restart_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    #[expect(
        clippy::await_holding_invalid_type,
        reason = "idle mailbox consumption is atomic with its durable checkpoint; the asynchronous queue lock excludes receivers and no storage operation calls back into this queue"
    )]
    pub(crate) async fn checkpoint_mailbox_for_eviction(
        &self,
        live_thread: Option<&codex_thread_store::LiveThread>,
        admission: &mut Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    ) -> codex_protocol::error::Result<Vec<InterAgentCommunication>> {
        let mut pending = self.mailbox_pending_mails.lock().await;
        self.read_mailbox(&mut pending);
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        for mail in pending
            .iter_mut()
            .take_while(|mail| !mail.communication.trigger_turn)
        {
            mail.communication
                .id
                .get_or_insert_with(|| codex_protocol::ResponseItemId::new("amsg"));
        }
        let mail: Vec<_> = pending
            .iter()
            .take_while(|mail| !mail.communication.trigger_turn)
            .map(|mail| mail.communication.clone())
            .collect();
        let persisted = async {
            let live_thread = live_thread.ok_or_else(|| {
                codex_protocol::error::CodexErr::InvalidRequest(
                    "cannot evict unread agent mail without canonical persistence".into(),
                )
            })?;
            live_thread
                .checkpoint_agent_mail(mail.clone())
                .await
                .map_err(|err| {
                    codex_protocol::error::CodexErr::Fatal(format!(
                        "failed to checkpoint unread agent mail: {err}"
                    ))
                })
        }
        .await;
        let mut retained_admission = self
            .mailbox_restart_admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if persisted.is_ok() {
            pending.drain(..mail.len());
        } else if retained_admission.is_none() {
            *retained_admission = admission.take();
        }
        persisted.map(|()| mail)
    }
}

impl Session {
    pub(crate) async fn reconcile_mailbox_before_consumption(
        self: &std::sync::Arc<Self>,
    ) -> codex_protocol::error::Result<()> {
        {
            let _pending = self.input_queue.mailbox_pending_mails.lock().await;
            if !self.input_queue.has_unresolved_mail_checkpoint() {
                return Ok(());
            }
        }
        let session = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            if session.input_queue.has_unresolved_mail_checkpoint() {
                session.checkpoint_mailbox_for_eviction(&mut None).await?;
            }
            Ok(())
        })
        .await
        .map_err(|err| {
            codex_protocol::error::CodexErr::Fatal(format!(
                "agent mail reconciliation task failed: {err}"
            ))
        })?
    }

    pub(crate) async fn checkpoint_mailbox_for_eviction(
        &self,
        admission: &mut Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    ) -> codex_protocol::error::Result<()> {
        let _boundary = self
            .code_mode_message_tasks
            .communication_boundary
            .acquire()
            .await
            .unwrap_or_else(|_| unreachable!("communication boundary remains open"));
        let mail = match self
            .input_queue
            .checkpoint_mailbox_for_eviction(self.services.live_thread.as_ref(), admission)
            .await
        {
            Ok(mail) => mail,
            Err(err) => {
                self.services.local_agent_runtime.refuse_restart_readiness();
                return Err(err);
            }
        };
        if !mail.is_empty() {
            let context = self.new_inject_items_context().await;
            let items: Vec<_> = mail
                .iter()
                .map(InterAgentCommunication::to_model_input_item)
                .collect();
            self.state
                .lock()
                .await
                .history
                .record_items(items.iter(), context.model_info().truncation_policy.into());
        }
        self.input_queue.finish_mail_checkpoint();
        Ok(())
    }
}
