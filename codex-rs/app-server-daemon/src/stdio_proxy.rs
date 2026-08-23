use std::path::Path;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use futures::SinkExt;
use futures::StreamExt;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

use crate::client;

/// Relays newline-delimited JSON-RPC between stdio and the app-server's
/// WebSocket control socket.
pub async fn proxy_stdio(socket_path: &Path) -> Result<()> {
    let websocket = client::connect(socket_path).await?;
    proxy_streams(websocket, tokio::io::stdin(), tokio::io::stdout()).await
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
    loop {
        tokio::select! {
            line = input_lines.next_line() => match line.context("failed to read proxy stdin")? {
                Some(line) => websocket
                    .send(Message::Text(line.into()))
                    .await
                    .context("failed to send app-server message")?,
                None => {
                    websocket.close(None).await.ok();
                    break;
                }
            },
            message = websocket.next() => match message {
                Some(Ok(Message::Text(text))) => {
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
                Some(Ok(Message::Close(_))) | None => break,
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
    use tokio::time::Duration;
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
}
