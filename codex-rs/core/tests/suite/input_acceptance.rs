// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use codex_core::StartIfIdleSubmission;
use codex_core::TurnInput;
use codex_core::TurnInputRequest;
use codex_core::TurnInputSubmission;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use test_case::test_case;

fn request(text: &str) -> TurnInputRequest {
    TurnInputRequest::new(TurnInput::UserInput {
        content: vec![UserInput::Text {
            text: text.to_string(),
            text_elements: Vec::new(),
        }],
        client_id: Some("durable-input".to_string()),
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_unaccepted_input_fences_live_and_cold_dispatch() -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_once(&server, responses::sse_completed("unexpected")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    assert!(
        test.codex
            .stop_identified_input("durable-input")
            .await?
            .is_none()
    );
    assert!(
        test.codex
            .start_or_steer_turn(request("never run"))
            .await
            .is_err()
    );
    let path = test.codex.rollout_path().expect("durable input fence");
    test.codex.submit(Op::Shutdown).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ShutdownComplete)
    })
    .await;
    let recovered = test_codex()
        .resume(&server, test.home.clone(), path)
        .await?;
    assert!(
        recovered
            .codex
            .start_or_steer_turn(request("never run"))
            .await
            .is_err()
    );
    assert!(
        recovered
            .codex
            .stop_identified_input("durable-input")
            .await?
            .is_none()
    );
    assert!(response.requests().is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identified_input_returns_the_original_acceptance_and_rejects_conflicts_after_completion()
-> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    let response = responses::mount_sse_once(&server, responses::sse_completed("accepted")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let accepted = test.codex.start_or_steer_turn(request("run once")).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert_eq!(
        test.codex.start_or_steer_turn(request("run once")).await?,
        accepted
    );
    let error = test
        .codex
        .start_or_steer_turn(request("different work"))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("already identifies different input")
    );
    let TurnInputSubmission::Started {
        turn_id,
        root_turn_id,
    } = accepted
    else {
        anyhow::bail!("expected a started turn");
    };
    assert_eq!(
        test.codex.start_turn_if_idle(request("run once")).await?,
        StartIfIdleSubmission::Started {
            turn_id,
            root_turn_id
        }
    );
    response.single_request();
    Ok(())
}

#[test_case(false; "recover exact accepted input before spawn")]
#[test_case(true; "stop accepted input before recovery")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identified_input_survives_loss_after_durable_acceptance(stop: bool) -> anyhow::Result<()> {
    let server = responses::start_mock_server().await;
    responses::mount_response_once(
        &server,
        responses::sse_response(responses::sse_completed("original"))
            .set_delay(std::time::Duration::from_secs(60)),
    )
    .await;
    let test = test_codex().build_with_auto_env(&server).await?;
    let accepted = test
        .codex
        .start_or_steer_turn(request("recover this exact input"))
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnStarted(_))
    })
    .await;
    let mut steer_request = request("recover the pending steer");
    if let TurnInput::UserInput { client_id, .. } = &mut steer_request.input {
        *client_id = Some("durable-steer".to_string());
    }
    let steered = test
        .codex
        .start_or_steer_turn(steer_request.clone())
        .await?;
    assert!(matches!(steered, TurnInputSubmission::Steered { .. }));
    assert_eq!(
        test.codex.start_or_steer_turn(steer_request).await?,
        steered
    );
    let TurnInputSubmission::Started { turn_id, .. } = &accepted else {
        anyhow::bail!("expected a started turn");
    };
    let path = test.codex.rollout_path().expect("durable rollout");
    test.codex.submit(Op::Shutdown).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ShutdownComplete)
    })
    .await;
    let history = std::fs::read_to_string(&path)?;
    let mut prefix = String::new();
    let mut captured_start = false;
    for line in history.lines() {
        let parsed: serde_json::Value = serde_json::from_str(line)?;
        let accepted_record =
            parsed["type"] == "input_acceptance" && parsed["payload"]["state"] == "accepted";
        if !captured_start || accepted_record {
            prefix.push_str(line);
            prefix.push('\n');
        }
        captured_start |= accepted_record;
    }
    std::fs::write(&path, prefix)?;
    let recovered_server = responses::start_mock_server().await;
    let response =
        responses::mount_sse_once(&recovered_server, responses::sse_completed("recovered")).await;
    let recovered = test_codex()
        .resume(&recovered_server, test.home.clone(), path)
        .await?;
    if stop {
        assert!(recovered.codex.interrupt_accepted_turn(turn_id).await?);
    }
    assert_eq!(
        recovered
            .codex
            .start_or_steer_turn(request("recover this exact input"))
            .await?,
        accepted
    );
    if stop {
        assert!(response.requests().is_empty());
    } else {
        let event = wait_for_event(&recovered.codex, |event| {
            matches!(event, EventMsg::TurnStarted(_))
        })
        .await;
        let EventMsg::TurnStarted(started) = event else {
            unreachable!()
        };
        assert_eq!(started.turn_id, *turn_id);
        wait_for_event(&recovered.codex, |event| {
            matches!(event, EventMsg::TurnComplete(_))
        })
        .await;
        assert!(
            response
                .single_request()
                .message_input_texts("user")
                .ends_with(&[
                    "recover this exact input".to_string(),
                    "recover the pending steer".to_string()
                ])
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identified_input_fails_closed_when_acceptance_history_has_an_unread_record()
-> anyhow::Result<()> {
    use std::io::Write;
    let server = responses::start_mock_server().await;
    responses::mount_sse_once(&server, responses::sse_completed("original")).await;
    let test = test_codex().build_with_auto_env(&server).await?;
    test.codex
        .start_or_steer_turn(request("already ran"))
        .await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    let path = test.codex.rollout_path().expect("durable rollout");
    test.codex.submit(Op::Shutdown).await?;
    wait_for_event(&test.codex, |event| {
        matches!(event, EventMsg::ShutdownComplete)
    })
    .await;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)?
        .write_all(b"{\"type\":\"input_acceptance\",\"payload\":\n")?;
    let recovered_server = responses::start_mock_server().await;
    let response =
        responses::mount_sse_once(&recovered_server, responses::sse_completed("unexpected")).await;
    let recovered = test_codex()
        .resume(&recovered_server, test.home.clone(), path)
        .await?;
    let error = recovered
        .codex
        .start_or_steer_turn(request("already ran"))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("unread records"), "{error}");
    assert!(response.requests().is_empty());
    Ok(())
}
