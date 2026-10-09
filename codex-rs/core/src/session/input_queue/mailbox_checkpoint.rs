use super::InputQueue;
use crate::session::Session;
use codex_protocol::protocol::InterAgentCommunication;

impl InputQueue {
    /// Consume idle native mail into canonical replay before residency drops its owner.
    /// The residency write guard excludes delivery and the caller owns the operation through
    /// cancellation. Failure leaves both input and restart admission with this queue.
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
        for mail in pending.iter_mut() {
            mail.communication
                .id
                .get_or_insert_with(|| codex_protocol::ResponseItemId::new("amsg"));
        }
        let mail: Vec<_> = pending
            .iter()
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
            pending.clear();
            retained_admission.take();
        } else if retained_admission.is_none() {
            *retained_admission = admission.take();
        }
        persisted.map(|()| mail)
    }
}

impl Session {
    pub(crate) async fn checkpoint_mailbox_for_eviction(
        &self,
        admission: &mut Option<tokio::sync::OwnedRwLockReadGuard<()>>,
    ) -> codex_protocol::error::Result<()> {
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
            // Preserve the live context too: a subsequent shutdown failure must not strand
            // checkpointed mail outside the still-registered session's model history.
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
        Ok(())
    }
}
