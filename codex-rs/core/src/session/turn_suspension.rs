// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::handlers;
use super::session::Session;
use super::turn::run_hooks_and_record_inputs;
use crate::state::TaskKind;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::turn_input::SuspendTurnOutcome;
use codex_thread_store::PersistContext;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub(super) async fn suspend_turn_and_shutdown(
    session: &Arc<Session>,
    submission_id: String,
    expected_turn_id: &str,
) -> CodexResult<SuspendTurnOutcome> {
    {
        let active = session.active_turn.lock().await;
        let Some(task) = active.as_ref().and_then(|turn| turn.task.as_ref()) else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
        if task.turn_context.sub_id != expected_turn_id {
            return Ok(SuspendTurnOutcome::TurnChanged {
                actual_turn_id: task.turn_context.sub_id.clone(),
            });
        }
    }

    // The process-wide restart fence prevents new descendants. Refuse a parent
    // until every currently active child has been suspended leaf-first.
    if session
        .services
        .local_agent_runtime
        .list_active_agent_subtree_thread_ids(session.thread_id)
        .await?
        .len()
        > 1
    {
        return Ok(SuspendTurnOutcome::HasLiveDescendants);
    }

    let live_thread = session
        .live_thread_for_persistence("suspend an unfinished turn")
        .map_err(|error| CodexErr::Fatal(error.to_string()))?;
    // Flush before canceling execution so a persistence failure leaves the original turn running.
    live_thread.flush().await.map_err(|error| {
        CodexErr::Fatal(format!("flush before turn suspension failed: {error}"))
    })?;

    session
        .prepare_accepted_input_restart(expected_turn_id)
        .await?;

    // The flush can yield while the active turn completes or changes. Recheck its
    // kind under the same lock used to remove it.
    let mut turn = {
        let mut active = session.active_turn.lock().await;
        let Some(active_turn) = active.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        let Some(task) = active_turn.task.as_ref() else {
            return Ok(SuspendTurnOutcome::NotActive);
        };
        if task.kind != TaskKind::Regular {
            return Ok(SuspendTurnOutcome::UnsupportedTask);
        }
        if task.turn_context.sub_id != expected_turn_id {
            return Ok(SuspendTurnOutcome::TurnChanged {
                actual_turn_id: task.turn_context.sub_id.clone(),
            });
        }
        active.take().ok_or_else(|| {
            CodexErr::Fatal("accepted turn suspension had no running turn".to_string())
        })?
    };

    let task = turn.task.take().ok_or_else(|| {
        CodexErr::Fatal("accepted turn suspension had no running task".to_string())
    })?;
    let turn_id = task.turn_context.sub_id.clone();
    // Normal shutdown records a terminal turn event, preventing another worker from
    // recovering this turn under its original ID. Cancel the task without that event.
    task.cancellation_token.cancel();
    task.turn_context
        .turn_metadata_state
        .cancel_git_enrichment_task();
    let mut task_handle = task.handle.detach();
    match tokio::time::timeout(
        Duration::from_millis(crate::tasks::GRACEFULL_INTERRUPTION_TIMEOUT_MS),
        &mut task_handle,
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            warn!(thread_id = %session.thread_id, %error, "suspended turn task exited abnormally");
        }
        Err(_) => {
            warn!(
                thread_id = %session.thread_id,
                "suspended turn task did not stop gracefully; aborting it"
            );
            task_handle.abort();
            let _ = task_handle.await;
        }
    }
    session.reconcile_mailbox_before_consumption().await?;
    let pending_input = session.input_queue.take_restart_input(&turn).await?;
    run_hooks_and_record_inputs(
        session,
        &task.turn_context,
        &task.turn_context.capture_current_model_info(),
        &pending_input,
        PersistContext::Standard,
    )
    .await;
    session.input_queue.clear_pending(&turn).await;

    // Stop all producers before flushing their final history and closing its writer.
    // If either persistence step fails, do not report success: the current worker
    // retains ownership until worker-failure recovery can take responsibility.
    handlers::shutdown_session_runtime(session).await;
    live_thread
        .flush()
        .await
        .map_err(|error| CodexErr::Fatal(format!("flush after turn suspension failed: {error}")))?;
    live_thread
        .shutdown()
        .await
        .map_err(|error| CodexErr::Fatal(format!("close suspended turn writer failed: {error}")))?;
    // Announce completion only after extension cleanup and writer closure so a
    // replacement worker cannot write the same thread concurrently.
    session
        .deliver_event_raw(Event {
            id: submission_id,
            msg: EventMsg::ShutdownComplete,
        })
        .await;
    Ok(SuspendTurnOutcome::Suspended { turn_id })
}
