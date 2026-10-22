// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
mod text;

pub(crate) use text::ObservationItems;

#[cfg(debug_assertions)]
#[expect(
    clippy::expect_used,
    reason = "A malformed or broken deterministic test barrier must fail its test process."
)]
pub(crate) async fn test_barrier(signal: &str, thread_id: codex_protocol::ThreadId) {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;
    let Ok(address) = std::env::var("CODEX_TEST_OBSERVATION_BARRIER") else {
        return;
    };
    let address: std::net::SocketAddr = address.parse().expect("test barrier address");
    assert!(
        address.ip().is_loopback(),
        "observation test barrier must be local"
    );
    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .expect("connect test barrier");
    stream
        .write_all(format!("{signal}:{thread_id}\n").as_bytes())
        .await
        .expect("signal test barrier");
    stream.read_u8().await.expect("release test barrier");
}
