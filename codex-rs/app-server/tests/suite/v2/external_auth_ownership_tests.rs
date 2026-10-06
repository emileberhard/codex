use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use app_test_support::ChatGptIdTokenClaims;
use app_test_support::encode_id_token;
use app_test_support::write_models_cache;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::JSONRPCResponse;
use core_test_support::responses;
use futures::SinkExt;
use pretty_assertions::assert_eq;
use serde_json::json;
use tempfile::TempDir;
use test_case::test_case;
use tokio::time::Duration;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;
use wiremock::matchers::method;
use wiremock::matchers::path;

use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server_with_args;

#[test_case("connected"; "only_the_login_connection_can_answer")]
#[test_case("disconnected"; "a_missing_owner_fails_without_broadcast")]
#[test_case("disconnect_pending"; "disconnect_cancels_a_pending_refresh")]
#[tokio::test]
async fn external_auth_refresh_connection_ownership(owner_state: &str) -> Result<()> {
    let home = TempDir::new()?;
    let backend = MockServer::start().await;
    app_test_support::mount_workspace_routing(&backend).await;
    Mock::given(method("GET"))
        .and(path("/backend-api/wham/settings/user"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "commit_attribution_enabled": false,
        })))
        .mount(&backend)
        .await;
    Mock::given(method("GET"))
        .and(path("/backend-api/wham/config/bundle"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&backend)
        .await;
    let inference = MockServer::start().await;
    let initial = encode_id_token(
        &ChatGptIdTokenClaims::new()
            .email("phase0@example.invalid")
            .chatgpt_user_id("phase0-user")
            .chatgpt_account_id("phase0-account")
            .plan_type("pro"),
    )?;
    let renewed = encode_id_token(
        &ChatGptIdTokenClaims::new()
            .email("renewed@example.invalid")
            .chatgpt_user_id("phase0-user")
            .chatgpt_account_id("phase0-account")
            .plan_type("pro"),
    )?;
    let mut response_sequence = vec![
        ResponseTemplate::new(401).set_body_json(json!({"error": {"message": "unauthorized"}})),
        responses::sse_response(responses::sse(vec![
            responses::ev_response_created("phase0-response"),
            responses::ev_assistant_message("phase0-message", "renewed"),
            responses::ev_completed("phase0-response"),
        ])),
    ];
    if owner_state != "connected" {
        response_sequence.truncate(1);
    }
    let requests = responses::mount_response_sequence(&inference, response_sequence).await;
    std::fs::write(
        home.path().join("config.toml"),
        format!(
            "model = \"mock-model\"\nmodel_provider = \"phase0\"\napproval_policy = \"never\"\nsandbox_mode = \"read-only\"\ncli_auth_credentials_store = \"file\"\nchatgpt_base_url = \"{}/backend-api\"\n[model_providers.phase0]\nname = \"OpenAI\"\nbase_url = \"{}/v1\"\nwire_api = \"responses\"\nrequires_openai_auth = true\nsupports_websockets = false\nrequest_max_retries = 0\nstream_max_retries = 0\n",
            backend.uri(),
            inference.uri(),
        ),
    )?;
    write_models_cache(home.path()).await?;
    let home_string = home.path().to_string_lossy().to_string();
    let (mut process, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[],
        &[("HOME", &home_string)],
    )
    .await?;
    let mut owner = connect_websocket(address).await?;
    let mut consumer = connect_websocket(address).await?;
    for (client, name) in [(&mut owner, "auth_owner"), (&mut consumer, "consumer")] {
        send_request(
            client,
            "initialize",
            /*id*/ 1,
            Some(json!({
                "clientInfo": {"name": name, "version": "phase0"},
                "capabilities": {"experimentalApi": true},
            })),
        )
        .await?;
        read_response_for_id(client, /*id*/ 1).await?;
    }
    send_request(
        &mut owner,
        "account/login/start",
        /*id*/ 2,
        Some(json!({
            "type": "chatgptAuthTokens", "accessToken": initial,
            "chatgptAccountId": "phase0-account", "chatgptPlanType": "pro",
        })),
    )
    .await?;
    assert_eq!(
        read_response_for_id(&mut owner, /*id*/ 2).await?.result,
        json!({"type": "chatgptAuthTokens"})
    );
    send_request(
        &mut consumer,
        "thread/start",
        /*id*/ 3,
        Some(json!({"model": "mock-model"})),
    )
    .await?;
    let thread = read_response_for_id(&mut consumer, /*id*/ 3).await?.result;
    if owner_state == "disconnected" {
        owner.close(None).await?;
    }
    send_request(
        &mut consumer,
        "turn/start",
        /*id*/ 4,
        Some(json!({
            "threadId": thread["thread"]["id"], "input": [{"type": "text", "text": "Hello"}],
        })),
    )
    .await?;
    if owner_state != "disconnected" {
        let refresh = loop {
            if let JSONRPCMessage::Request(request) = read_jsonrpc_message(&mut owner).await? {
                assert_eq!(request.method, "account/chatgptAuthTokens/refresh");
                break request;
            }
        };
        if owner_state == "disconnect_pending" {
            owner.close(None).await?;
        } else {
            consumer
                .send(Message::Text(
                    serde_json::to_string(&json!({
                        "id": refresh.id, "error": {"code": -32601, "message": "not the owner"},
                    }))?
                    .into(),
                ))
                .await?;
            send_request(
                &mut consumer,
                "account/read",
                /*id*/ 5,
                Some(json!({"refreshToken": false})),
            )
            .await?;
            read_response_for_id(&mut consumer, /*id*/ 5).await?;
            owner.send(Message::Text(serde_json::to_string(&JSONRPCResponse {
                id: refresh.id,
                result: json!({"accessToken": renewed, "chatgptAccountId": "phase0-account", "chatgptPlanType": "pro"}),
            })?.into())).await?;
        }
    }
    let completed = timeout(Duration::from_secs(5), async {
        loop {
            match read_jsonrpc_message(&mut consumer).await? {
                JSONRPCMessage::Request(request) => {
                    bail!("refresh leaked to consumer: {}", request.method)
                }
                JSONRPCMessage::Notification(notification)
                    if notification.method == "turn/completed" =>
                {
                    return notification.params.context("turn/completed params");
                }
                _ => {}
            }
        }
    })
    .await??;
    assert_eq!(
        completed["turn"]["status"],
        if owner_state == "connected" {
            "completed"
        } else {
            "failed"
        }
    );
    assert_eq!(
        requests.requests().len(),
        if owner_state == "connected" { 2 } else { 1 }
    );
    assert!(!home.path().join("auth.json").exists());
    process.kill().await?;
    Ok(())
}
