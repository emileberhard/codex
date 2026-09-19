use std::os::unix::fs::symlink;
use std::os::unix::net::UnixListener;

use tempfile::TempDir;

use super::Daemon;

fn daemon(temp_dir: &TempDir) -> Daemon {
    Daemon {
        socket_path: temp_dir.path().join("control.sock"),
        pid_file: temp_dir.path().join("app-server.pid"),
        operation_lock_file: temp_dir.path().join("daemon.lock"),
        settings_file: temp_dir.path().join("settings.json"),
        managed_codex_bin: temp_dir.path().join("missing-codex"),
    }
}

#[tokio::test]
async fn start_defers_refused_socket_cleanup_to_managed_transport() {
    let temp_dir = TempDir::new().expect("temp dir");
    let daemon = daemon(&temp_dir);
    let listener = UnixListener::bind(&daemon.socket_path).expect("bind socket");
    drop(listener);

    let error = daemon.start().await.expect_err("managed binary is absent");
    assert!(
        error
            .to_string()
            .contains("managed standalone Codex install not found"),
        "startup should pass the socket gate: {error:#}"
    );
    assert!(
        daemon.socket_path.exists(),
        "only the transport removes stale sockets"
    );
    daemon.ensure_app_server_absent().await.expect_err(
        "restart preparation still requires positive absence, even for a refused socket",
    );
}

#[tokio::test]
async fn start_preserves_listener_that_does_not_answer_protocol_probes() {
    let temp_dir = TempDir::new().expect("temp dir");
    let daemon = daemon(&temp_dir);
    let _listener = UnixListener::bind(&daemon.socket_path).expect("bind socket");

    let error = daemon
        .start()
        .await
        .expect_err("unverified listener blocks startup");
    assert!(
        error
            .to_string()
            .contains("exists but its owner could not be verified"),
        "unexpected error: {error:#}"
    );
    assert!(daemon.socket_path.exists());
}

#[tokio::test]
async fn start_preserves_symlink_to_refused_socket() {
    let temp_dir = TempDir::new().expect("temp dir");
    let daemon = daemon(&temp_dir);
    let target = temp_dir.path().join("other.sock");
    drop(UnixListener::bind(&target).expect("bind socket"));
    symlink(&target, &daemon.socket_path).expect("link socket");

    let error = daemon.start().await.expect_err("symlink blocks startup");
    assert!(
        error
            .to_string()
            .contains("exists but its owner could not be verified"),
        "unexpected error: {error:#}"
    );
    assert!(daemon.socket_path.is_symlink());
    assert!(target.exists());
}
