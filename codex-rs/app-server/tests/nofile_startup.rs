#![cfg(unix)]
// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.

use pretty_assertions::assert_eq;
use std::os::unix::fs::PermissionsExt;
use std::time::Duration;
use tokio::process::Command;

#[tokio::test]
async fn insufficient_hard_limit_fails_before_restart_grant_consumption_or_listener_start() {
    if current_nofile_limit().rlim_max < 4_096 {
        return;
    }

    let temp = tempfile::tempdir().expect("create test directory");
    let grant_path = temp.path().join("restart-grant.json");
    std::fs::write(
        &grant_path,
        r#"{"schemaVersion":1,"generationId":"generation-1","capability":"0123456789abcdef0123456789abcdef","targets":[]}"#,
    )
    .expect("write restart grant");
    let mut permissions = std::fs::metadata(&grant_path)
        .expect("read restart grant metadata")
        .permissions();
    permissions.set_mode(0o600);
    std::fs::set_permissions(&grant_path, permissions).expect("make restart grant private");

    let socket_path = temp.path().join("app-server.sock");
    let listen_url = format!("unix://{}", socket_path.display());
    let binary = codex_utils_cargo_bin::cargo_bin("codex-app-server")
        .expect("locate app-server test binary");
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        Command::new("/bin/sh")
            .kill_on_drop(true)
            .args([
                "-c",
                "ulimit -S -n 4096 && ulimit -H -n 4096 && exec \"$@\"",
                "nofile-startup-test",
            ])
            .arg(binary)
            .arg("--listen")
            .arg(&listen_url)
            .env("CODEX_HOME", temp.path())
            .env("CODEX_APP_SERVER_START_FENCED", "1")
            .env("CODEX_INTERNAL_RESTART_RECOVERY_GRANT_FILE", &grant_path)
            .env("CODEX_APP_SERVER_DISABLE_MANAGED_CONFIG", "1")
            .output(),
    )
    .await
    .expect("app-server did not reject the low hard limit within ten seconds")
    .expect("spawn app-server with a low hard limit");

    assert!(!output.status.success(), "app-server unexpectedly started");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("hard limit 4096"),
        "missing resource-limit error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        grant_path.exists(),
        "startup grant was consumed before failure"
    );
    assert!(
        !socket_path.exists(),
        "app-server listener started before failure"
    );
}

fn current_nofile_limit() -> libc::rlimit {
    // SAFETY: `limit` points to writable storage for the operating system to fill.
    let mut limit = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) },
        0
    );
    limit
}
