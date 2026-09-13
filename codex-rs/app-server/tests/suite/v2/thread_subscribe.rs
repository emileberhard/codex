use super::connection_handling_websocket::WsClient;
use super::connection_handling_websocket::connect_websocket;
use super::connection_handling_websocket::create_config_toml;
use super::connection_handling_websocket::read_jsonrpc_message;
use super::connection_handling_websocket::read_response_for_id;
use super::connection_handling_websocket::send_request;
use super::connection_handling_websocket::spawn_websocket_server;
use super::connection_handling_websocket::spawn_websocket_server_with_args;
use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::JSONRPCMessage;
use codex_app_server_protocol::ThreadItem;
use codex_app_server_protocol::ThreadItemsListResponse;
use codex_app_server_protocol::ThreadStartResponse;
use codex_app_server_protocol::ThreadSubscribeResponse;
use core_test_support::responses;
use core_test_support::streaming_sse::StreamingSseChunk;
use core_test_support::streaming_sse::start_streaming_sse_server;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::time::Duration;
use tokio::time::timeout;

const DEADLINE: Duration = Duration::from_secs(60);

async fn initialize(client: &mut WsClient) -> Result<()> {
    send_request(
        client,
        "initialize",
        1,
        Some(json!({
            "clientInfo":{"name":"subscription_test","version":"1"},
            "capabilities":{"experimentalApi":true}
        })),
    )
    .await?;
    read_response_for_id(client, 1).await?;
    Ok(())
}

async fn start(client: &mut WsClient, ephemeral: bool) -> Result<ThreadStartResponse> {
    send_request(
        client,
        "thread/start",
        2,
        Some(json!({"model":"mock-model","ephemeral":ephemeral})),
    )
    .await?;
    Ok(serde_json::from_value(
        read_response_for_id(client, 2).await?.result,
    )?)
}

async fn gate(listener: &TcpListener, expected: &str) -> Result<TcpStream> {
    let (stream, _) = timeout(DEADLINE, listener.accept()).await??;
    let mut stream = BufReader::new(stream);
    let mut line = String::new();
    timeout(DEADLINE, stream.read_line(&mut line)).await??;
    assert_eq!(line.trim(), expected);
    Ok(stream.into_inner())
}

#[derive(Clone, Copy)]
enum CompletionTiming {
    AfterSubscribe,
    BeforeSubscribe,
    NextTurnBeforeSubscribe,
}

#[tokio::test]
async fn subscription_seeds_partial_text_before_subsequent_deltas() -> Result<()> {
    exercise_checkpoint(CompletionTiming::AfterSubscribe, "paginated").await
}

#[tokio::test]
async fn subscription_covers_persisted_completion_queued_behind_listener() -> Result<()> {
    exercise_checkpoint(CompletionTiming::BeforeSubscribe, "paginated").await
}

#[tokio::test]
async fn subscription_covers_next_turn_persisted_ahead_of_listener() -> Result<()> {
    exercise_checkpoint(CompletionTiming::NextTurnBeforeSubscribe, "paginated").await
}

#[tokio::test]
async fn legacy_subscription_seeds_partial_text() -> Result<()> {
    exercise_checkpoint(CompletionTiming::AfterSubscribe, "legacy").await
}

#[tokio::test]
async fn legacy_subscription_covers_completed_canonical_identity() -> Result<()> {
    exercise_checkpoint(CompletionTiming::BeforeSubscribe, "legacy").await
}

async fn exercise_checkpoint(timing: CompletionTiming, history_mode: &str) -> Result<()> {
    let barrier = TcpListener::bind("127.0.0.1:0").await?;
    let barrier_address = barrier.local_addr()?.to_string();
    let (finish, finish_gate) = oneshot::channel();
    let (server, _) = start_streaming_sse_server(vec![
        vec![
            StreamingSseChunk {
                gate: None,
                body: responses::sse(vec![
                    responses::ev_response_created("response-1"),
                    responses::ev_message_item_added("observation-barrier", ""),
                    responses::ev_output_text_delta("before "),
                ]),
            },
            StreamingSseChunk {
                gate: Some(finish_gate),
                body: responses::sse(vec![
                    responses::ev_output_text_delta("after"),
                    responses::ev_assistant_message("observation-barrier", "before after"),
                    responses::ev_completed("response-1"),
                ]),
            },
        ],
        vec![StreamingSseChunk {
            gate: None,
            body: responses::sse(vec![
                responses::ev_response_created("response-2"),
                responses::ev_assistant_message("second-message", "second turn"),
                responses::ev_completed("response-2"),
            ]),
        }],
    ])
    .await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), server.uri(), "never")?;
    let (_process, address) = spawn_websocket_server_with_args(
        home.path(),
        "ws://127.0.0.1:0",
        &[],
        &[("CODEX_TEST_OBSERVATION_BARRIER", &barrier_address)],
    )
    .await?;
    let mut interactive = connect_websocket(address).await?;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut interactive).await?;
    initialize(&mut observer).await?;
    send_request(
        &mut interactive,
        "thread/start",
        2,
        Some(json!({"model":"mock-model","historyMode":history_mode})),
    )
    .await?;
    let thread = serde_json::from_value::<ThreadStartResponse>(
        read_response_for_id(&mut interactive, 2).await?.result,
    )?
    .thread;
    send_request(
        &mut interactive,
        "turn/start",
        3,
        Some(json!({"threadId":thread.id,"input":[{"type":"text","text":"first"}]})),
    )
    .await?;
    let turn = read_response_for_id(&mut interactive, 3).await?.result["turn"]["id"]
        .as_str()
        .context("turn id")?
        .to_owned();
    let mut listener_gate = gate(&barrier, &format!("listener:{}", thread.id)).await?;
    let mut finish = Some(finish);
    if !matches!(timing, CompletionTiming::AfterSubscribe) {
        finish.take().unwrap().send(()).unwrap();
        wait_for_completed_item(&mut observer, &thread.id, "observation-barrier").await?;
        if history_mode == "paginated" {
            timeout(DEADLINE, async {
                loop {
                    send_request(
                        &mut observer,
                        "thread/turns/list",
                        900,
                        Some(json!({"threadId":thread.id,"itemsView":"notLoaded"})),
                    )
                    .await?;
                    let page = read_response_for_id(&mut observer, 900).await?.result;
                    if page["data"]
                        .as_array()
                        .context("turns")?
                        .iter()
                        .any(|entry| entry["id"] == turn && entry["status"] == "completed")
                    {
                        break;
                    }
                }
                anyhow::Ok(())
            })
            .await??;
        }
    }
    if matches!(timing, CompletionTiming::NextTurnBeforeSubscribe) {
        send_request(
            &mut interactive,
            "turn/start",
            4,
            Some(json!({"threadId":thread.id,"input":[{"type":"text","text":"second"}]})),
        )
        .await?;
        read_response_for_id(&mut interactive, 4).await?;
        wait_for_completed_item(&mut observer, &thread.id, "second-message").await?;
    }
    send_request(
        &mut observer,
        "thread/subscribe",
        1000,
        Some(json!({"threadId":thread.id})),
    )
    .await?;
    let mut queued_gate = gate(&barrier, &format!("subscribe:{}", thread.id)).await?;
    queued_gate.write_all(&[1]).await?;
    listener_gate.write_all(&[1]).await?;
    let response: ThreadSubscribeResponse =
        serde_json::from_value(read_response_for_id(&mut observer, 1000).await?.result)?;
    let ThreadSubscribeResponse::Subscribed { checkpoint, .. } = response else {
        bail!("loaded thread was not subscribed")
    };
    assert_eq!(
        checkpoint.active_turn.as_ref().map(|turn| turn.id.as_str()),
        Some(turn.as_str())
    );
    assert_eq!(checkpoint.live_items.len(), 1);
    let ThreadItem::AgentMessage { text: prefix, .. } = &checkpoint.live_items[0].item else {
        bail!("message prefix missing")
    };
    assert_eq!(prefix, "before ");

    send_request(&mut observer, "thread/items/list", 1001, Some(json!({"threadId":thread.id,"cursor":checkpoint.items_backwards_cursor,"limit":100,"sortDirection":"desc"}))).await?;
    // Read the page response without throwing away the intervening live events.
    let mut deltas = String::new();
    let mut completions = Vec::new();
    let mut page = None;
    let expect_terminal_count = if matches!(timing, CompletionTiming::NextTurnBeforeSubscribe) {
        2
    } else {
        1
    };
    if let Some(finish) = finish {
        finish.send(()).unwrap();
    }
    timeout(DEADLINE, async {
        while page.is_none() || completions.len() < expect_terminal_count {
            match read_jsonrpc_message(&mut observer).await? {
                JSONRPCMessage::Response(response)
                    if response.id == codex_app_server_protocol::RequestId::Integer(1001) =>
                {
                    page = Some(serde_json::from_value::<ThreadItemsListResponse>(
                        response.result,
                    )?);
                }
                JSONRPCMessage::Notification(note) => {
                    let params = note.params.unwrap_or(Value::Null);
                    if note.method == "item/agentMessage/delta"
                        && params["itemId"] == "observation-barrier"
                    {
                        deltas.push_str(params["delta"].as_str().context("delta")?);
                    }
                    if note.method == "turn/completed" {
                        completions.push(params);
                    }
                }
                JSONRPCMessage::Request(request) => {
                    bail!("passive observer received request: {}", request.method)
                }
                JSONRPCMessage::Error(error) => bail!("RPC error: {:?}", error),
                _ => {}
            }
        }
        anyhow::Ok(())
    })
    .await??;
    let page = page.context("checkpoint items")?;
    if history_mode == "legacy" && matches!(timing, CompletionTiming::AfterSubscribe) {
        assert!(
            !page
                .data
                .iter()
                .any(|entry| entry.item.id() == "observation-barrier")
        );
        assert_eq!(format!("{prefix}{deltas}"), "before after");
        return Ok(());
    }
    let message = page
        .data
        .iter()
        .find(|entry| entry.item.id() == "observation-barrier")
        .context("checkpoint message")?;
    let ThreadItem::AgentMessage { text: durable, .. } = &message.item else {
        bail!("wrong checkpoint item")
    };
    if matches!(timing, CompletionTiming::AfterSubscribe) {
        assert_eq!(message.is_complete, Some(false));
        assert_eq!(format!("{prefix}{deltas}"), "before after");
    } else {
        assert_eq!(message.is_complete, Some(true));
        assert_eq!(durable, "before after");
        assert_eq!(deltas, "after");
    }
    assert!(message.started_at_ordinal.is_some());
    if matches!(timing, CompletionTiming::NextTurnBeforeSubscribe) {
        assert!(
            page.data
                .iter()
                .any(|entry| entry.item.id() == "second-message")
        );
    }
    Ok(())
}

async fn wait_for_completed_item(
    client: &mut WsClient,
    thread_id: &str,
    item_id: &str,
) -> Result<()> {
    timeout(DEADLINE, async {
        let mut id = 20;
        loop {
            send_request(
                client,
                "thread/items/list",
                id,
                Some(json!({"threadId":thread_id,"limit":100,"sortDirection":"desc"})),
            )
            .await?;
            let page: ThreadItemsListResponse =
                serde_json::from_value(read_response_for_id(client, id).await?.result)?;
            if page
                .data
                .iter()
                .any(|entry| entry.item.id() == item_id && entry.is_complete == Some(true))
            {
                return Ok(());
            }
            id += 1;
            tokio::task::yield_now().await;
        }
    })
    .await?
}

#[tokio::test]
async fn subscription_does_not_materialize_fresh_or_ephemeral_threads() -> Result<()> {
    let server = app_test_support::create_mock_responses_server_repeating_assistant("unused").await;
    let home = TempDir::new()?;
    create_config_toml(home.path(), &server.uri(), "never")?;
    let (_process, address) = spawn_websocket_server(home.path()).await?;
    let mut interactive = connect_websocket(address).await?;
    let mut observer = connect_websocket(address).await?;
    initialize(&mut interactive).await?;
    initialize(&mut observer).await?;
    for ephemeral in [false, true] {
        let thread = start(&mut interactive, ephemeral).await?.thread;
        if let Some(path) = &thread.path {
            assert!(!path.exists());
        }
        send_request(
            &mut observer,
            "thread/subscribe",
            9,
            Some(json!({"threadId":thread.id})),
        )
        .await?;
        let response: ThreadSubscribeResponse =
            serde_json::from_value(read_response_for_id(&mut observer, 9).await?.result)?;
        let ThreadSubscribeResponse::Subscribed { checkpoint, .. } = response else {
            bail!("fresh runtime not observed")
        };
        assert_eq!(checkpoint.turns_backwards_cursor, None);
        assert_eq!(checkpoint.items_backwards_cursor, None);
        assert!(checkpoint.live_items.is_empty());
        if let Some(path) = &thread.path {
            assert!(!path.exists());
        }
    }
    send_request(
        &mut observer,
        "thread/subscribe",
        10,
        Some(json!({"threadId":codex_protocol::ThreadId::default().to_string()})),
    )
    .await?;
    let result: ThreadSubscribeResponse =
        serde_json::from_value(read_response_for_id(&mut observer, 10).await?.result)?;
    assert_eq!(result, ThreadSubscribeResponse::NotLoaded);
    Ok(())
}
