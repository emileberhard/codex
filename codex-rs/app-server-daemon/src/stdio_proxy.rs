// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::collections::HashSet;
use std::io::ErrorKind;
use std::io::Read;
use std::path::Path;
use std::pin::Pin;
use std::task::Poll;
use std::task::ready;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use codex_app_server_protocol::JSONRPCMessage;
use futures::SinkExt;
use futures::StreamExt;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::io::ReadBuf;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio::time::sleep_until;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::client;

const EOF_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const INPUT_CHUNK_BYTES: usize = 64 * 1024;
const INPUT_QUEUE_CHUNKS: usize = 16;

/// Relays newline-delimited JSON-RPC between stdio and the app-server's
/// WebSocket control socket.
pub async fn proxy_stdio(socket_path: &Path) -> Result<()> {
    let websocket = client::connect(socket_path).await?;
    proxy_streams(
        websocket,
        DetachedInput::spawn(std::io::stdin()),
        tokio::io::stdout(),
    )
    .await
}

struct DetachedInput {
    chunks: mpsc::Receiver<std::io::Result<Vec<u8>>>,
    chunk: Vec<u8>,
    offset: usize,
}

impl DetachedInput {
    fn spawn(mut source: impl Read + Send + 'static) -> Self {
        let (sender, chunks) = mpsc::channel(INPUT_QUEUE_CHUNKS);
        std::thread::spawn(move || {
            let mut buffer = vec![0; INPUT_CHUNK_BYTES];
            loop {
                let chunk = match source.read(&mut buffer) {
                    Ok(0) => return,
                    Ok(read) => Ok(buffer[..read].to_vec()),
                    Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                    Err(error) => Err(error),
                };
                let failed = chunk.is_err();
                if sender.blocking_send(chunk).is_err() || failed {
                    return;
                }
            }
        });
        Self {
            chunks,
            chunk: Vec::new(),
            offset: 0,
        }
    }
}

impl AsyncRead for DetachedInput {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let input = self.get_mut();
        while input.offset == input.chunk.len() {
            match ready!(input.chunks.poll_recv(cx)) {
                Some(Ok(chunk)) => {
                    input.chunk = chunk;
                    input.offset = 0;
                }
                Some(Err(error)) => return Poll::Ready(Err(error)),
                None => return Poll::Ready(Ok(())),
            }
        }
        let available = &input.chunk[input.offset..];
        let count = available.len().min(buf.remaining());
        buf.put_slice(&available[..count]);
        input.offset += count;
        Poll::Ready(Ok(()))
    }
}

async fn proxy_streams<S, R, W>(
    mut websocket: WebSocketStream<S>,
    input: R,
    mut output: W,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut input_lines = BufReader::new(input).lines();
    let mut input_closed = false;
    let mut pending_requests = HashSet::new();
    let mut drain_deadline = Instant::now();
    loop {
        if input_closed && pending_requests.is_empty() {
            websocket.close(None).await.ok();
            break;
        }

        tokio::select! {
            line = input_lines.next_line(), if !input_closed => match line.context("failed to read proxy stdin")? {
                Some(line) => {
                    if let Ok(JSONRPCMessage::Request(request)) = serde_json::from_str(&line)
                    {
                        pending_requests.insert(request.id);
                    }
                    websocket
                        .send(Message::Text(line.into()))
                        .await
                        .context("failed to send app-server message")?;
                }
                None => {
                    input_closed = true;
                    drain_deadline = Instant::now() + EOF_DRAIN_TIMEOUT;
                }
            },
            () = sleep_until(drain_deadline), if input_closed => {
                bail!(
                    "timed out draining {} app-server response(s) after proxy stdin closed",
                    pending_requests.len()
                );
            },
            message = websocket.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if let Ok(message) = serde_json::from_str(&text) {
                        match message {
                            JSONRPCMessage::Response(response) => {
                                pending_requests.remove(&response.id);
                            }
                            JSONRPCMessage::Error(error) => {
                                pending_requests.remove(&error.id);
                            }
                            JSONRPCMessage::Request(_) | JSONRPCMessage::Notification(_) => {}
                        }
                    }
                    output
                        .write_all(text.as_bytes())
                        .await
                        .context("failed to write proxy stdout")?;
                    output
                        .write_all(b"\n")
                        .await
                        .context("failed to terminate proxy output line")?;
                    output.flush().await.context("failed to flush proxy stdout")?;
                }
                Some(Ok(Message::Ping(payload))) => websocket
                    .send(Message::Pong(payload))
                    .await
                    .context("failed to answer app-server ping")?,
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None => {
                    if input_closed && !pending_requests.is_empty() {
                        bail!(
                            "app-server closed with {} pending response(s) after proxy stdin closed",
                            pending_requests.len()
                        );
                    }
                    break;
                },
                Some(Ok(Message::Binary(_))) => {
                    bail!("app-server sent unexpected binary WebSocket content")
                }
                Some(Ok(Message::Frame(_))) => {}
                Some(Err(err)) => return Err(err).context("app-server WebSocket failed"),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_uds::UnixListener;
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;
    use tokio::io::AsyncBufReadExt;
    use tokio::io::AsyncWriteExt;
    use tokio::io::BufReader;
    use tokio::time::timeout;
    use tokio_tungstenite::accept_async;

    #[tokio::test]
    async fn bridges_json_lines_through_a_websocket_control_socket() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let socket_path = temp_dir.path().join("app-server.sock");
        let mut listener = UnixListener::bind(&socket_path).await?;

        let server = tokio::spawn(async move {
            let stream = listener.accept().await?;
            let mut websocket = accept_async(stream).await?;
            assert_eq!(
                websocket.next().await.transpose()?,
                Some(Message::Text("request".into()))
            );
            websocket.send(Message::Text("response".into())).await?;
            websocket.close(None).await?;
            anyhow::Ok(())
        });

        let websocket = client::connect(&socket_path).await?;
        let (mut input_writer, input_reader) = tokio::io::duplex(1024);
        let (output_writer, output_reader) = tokio::io::duplex(1024);
        let proxy = tokio::spawn(proxy_streams(websocket, input_reader, output_writer));

        input_writer.write_all(b"request\n").await?;
        let mut response = String::new();
        timeout(
            Duration::from_secs(1),
            BufReader::new(output_reader).read_line(&mut response),
        )
        .await??;
        assert_eq!(response, "response\n");

        timeout(Duration::from_secs(1), server).await???;
        timeout(Duration::from_secs(1), proxy).await???;
        Ok(())
    }

    #[tokio::test]
    async fn drains_pending_response_after_input_eof() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let socket_path = temp_dir.path().join("app-server.sock");
        let mut listener = UnixListener::bind(&socket_path).await?;

        let server = tokio::spawn(async move {
            let stream = listener.accept().await?;
            let mut websocket = accept_async(stream).await?;
            assert_eq!(
                websocket.next().await.transpose()?,
                Some(Message::Text(r#"{"id":1,"method":"config/read"}"#.into()))
            );
            websocket
                .send(Message::Text(r#"{"id":1,"result":{}}"#.into()))
                .await?;
            websocket.close(None).await?;
            anyhow::Ok(())
        });

        let websocket = client::connect(&socket_path).await?;
        let (mut input_writer, input_reader) = tokio::io::duplex(1024);
        let (output_writer, output_reader) = tokio::io::duplex(1024);
        let proxy = tokio::spawn(proxy_streams(websocket, input_reader, output_writer));

        input_writer
            .write_all(b"{\"id\":1,\"method\":\"config/read\"}\n")
            .await?;
        drop(input_writer);

        let mut response = String::new();
        timeout(
            Duration::from_secs(1),
            BufReader::new(output_reader).read_line(&mut response),
        )
        .await??;
        assert_eq!(response, "{\"id\":1,\"result\":{}}\n");

        timeout(Duration::from_secs(1), server).await???;
        timeout(Duration::from_secs(1), proxy).await???;
        Ok(())
    }

    #[test]
    fn runtime_shuts_down_while_detached_input_is_still_open() -> Result<()> {
        let (input, _input_writer) = std::io::pipe()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(async {
            let temp_dir = TempDir::new()?;
            let socket_path = temp_dir.path().join("app-server.sock");
            let mut listener = UnixListener::bind(&socket_path).await?;
            let server = tokio::spawn(async move {
                let stream = listener.accept().await?;
                let mut websocket = accept_async(stream).await?;
                websocket.close(None).await?;
                anyhow::Ok(())
            });

            let websocket = client::connect(&socket_path).await?;
            timeout(
                Duration::from_secs(1),
                proxy_streams(websocket, DetachedInput::spawn(input), tokio::io::sink()),
            )
            .await??;
            timeout(Duration::from_secs(1), server).await???;
            anyhow::Ok(())
        })?;

        let (shut_down, shutdown) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            drop(runtime);
            shut_down.send(()).ok();
        });
        shutdown.recv_timeout(Duration::from_secs(5))?;
        Ok(())
    }
}
