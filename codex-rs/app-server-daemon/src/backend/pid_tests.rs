use std::process::Stdio;
use std::time::Duration;

use pretty_assertions::assert_eq;
use tempfile::TempDir;

use codex_app_server_transport::REMOTE_CONTROL_DISABLED_ENV_VAR;

use super::PidBackend;
use super::PidFileState;
use super::PidLogTail;
use super::PidRecord;
use super::process_matches_record;
use super::read_process_start_time;
use super::read_process_state;
use super::read_stderr_log_tail;
use super::stderr_log_file_for_pid_file;
use super::try_lock_file;

#[tokio::test]
async fn locked_empty_pid_file_is_treated_as_active_reservation() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    tokio::fs::write(&pid_file, "")
        .await
        .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );
    let reservation = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&backend.lock_file)
        .await
        .expect("open pid lock file");
    assert!(try_lock_file(&reservation).expect("lock reservation"));

    assert_eq!(
        backend.read_pid_file_state().await.expect("read pid"),
        PidFileState::Starting
    );
    assert!(pid_file.exists());
}

#[tokio::test]
async fn unlocked_empty_pid_file_is_treated_as_stale_reservation() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    tokio::fs::write(&pid_file, "")
        .await
        .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );

    assert_eq!(
        backend.read_pid_file_state().await.expect("read pid"),
        PidFileState::Missing
    );
    assert!(!pid_file.exists());
}

#[tokio::test]
async fn stop_waits_for_live_reservation_to_resolve() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    tokio::fs::write(&pid_file, "")
        .await
        .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );
    let reservation = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&backend.lock_file)
        .await
        .expect("open pid lock file");
    assert!(try_lock_file(&reservation).expect("lock reservation"));
    let cleanup = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(reservation);
        tokio::fs::remove_file(pid_file)
            .await
            .expect("remove pid file");
    });

    backend.stop().await.expect("stop");
    cleanup.await.expect("cleanup task");
}

#[tokio::test]
async fn start_retries_stale_empty_pid_file_under_its_own_lock() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    tokio::fs::write(&pid_file, "")
        .await
        .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("missing-codex"),
        pid_file,
        /*remote_control_enabled*/ false,
    );

    let err = backend.start().await.expect_err("start");
    assert!(
        err.to_string()
            .starts_with("failed to spawn detached app-server process using ")
    );
}

#[tokio::test]
async fn stale_record_cleanup_preserves_replacement_record() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );
    let stale = PidRecord {
        pid: 1,
        process_start_time: "old".to_string(),
    };
    let replacement = PidRecord {
        pid: 2,
        process_start_time: "new".to_string(),
    };
    tokio::fs::write(
        &pid_file,
        serde_json::to_vec(&replacement).expect("serialize replacement"),
    )
    .await
    .expect("write replacement pid file");

    assert_eq!(
        backend
            .refresh_after_stale_record(&stale)
            .await
            .expect("cleanup"),
        PidFileState::Running(replacement)
    );
}

#[tokio::test]
async fn stop_reaps_untracked_app_server_child() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let mut child = std::process::Command::new("sleep")
        .arg("5")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn app-server shim");
    let pid = child.id();
    let record = PidRecord {
        pid,
        process_start_time: read_process_start_time(pid).await.expect("start time"),
    };
    tokio::fs::write(
        &pid_file,
        serde_json::to_vec(&record).expect("serialize pid"),
    )
    .await
    .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );

    let result = tokio::time::timeout(Duration::from_secs(2), backend.stop()).await;
    if matches!(child.try_wait(), Ok(None)) {
        let _ = child.kill();
        let _ = child.wait();
    }

    // `sleep` is not tracked by Tokio, so stop must reap it instead of leaving a zombie.
    result.expect("stop timed out").expect("stop");
    assert!(!pid_file.exists());
}

#[tokio::test]
async fn stop_handles_an_orphaned_app_server_exit() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let output = std::process::Command::new("sh")
        .args(["-c", "sleep 5 >/dev/null 2>&1 & echo $!"])
        .output()
        .expect("spawn orphaned app-server shim");
    assert!(output.status.success());
    let pid = String::from_utf8(output.stdout)
        .expect("pid output is UTF-8")
        .trim()
        .parse()
        .expect("pid output is numeric");
    let record = PidRecord {
        pid,
        process_start_time: read_process_start_time(pid).await.expect("start time"),
    };
    tokio::fs::write(
        &pid_file,
        serde_json::to_vec(&record).expect("serialize pid"),
    )
    .await
    .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );

    tokio::time::timeout(Duration::from_secs(2), backend.stop())
        .await
        .expect("stop timed out")
        .expect("stop");

    assert!(!pid_file.exists());
}

#[tokio::test]
async fn zombie_process_is_not_an_active_pid_record() {
    let child = std::process::Command::new("sh")
        .arg("-c")
        .arg("sleep 0.1")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");
    let pid = child.id();
    let record = PidRecord {
        pid,
        process_start_time: read_process_start_time(pid).await.expect("start time"),
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if read_process_state(pid)
            .await
            .is_ok_and(|state| state.starts_with('Z'))
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "child never became a zombie"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(
        !process_matches_record(&record)
            .await
            .expect("inspect record")
    );
    assert!(
        read_process_state(pid).await.is_err(),
        "zombie child remained after ownership inspection"
    );
    drop(child);
}

#[tokio::test]
async fn recorded_start_time_matches_the_upstream_ps_format() {
    let pid = std::process::id();
    let output = tokio::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "lstart="])
        .output()
        .await
        .expect("invoke ps");
    assert!(output.status.success());

    let expected = String::from_utf8(output.stdout)
        .expect("ps output is UTF-8")
        .trim()
        .to_string();
    assert_eq!(
        read_process_start_time(pid).await.expect("start time"),
        expected
    );
}

#[tokio::test]
async fn live_pid_with_a_different_start_time_fails_closed() {
    let mut child = std::process::Command::new("sleep")
        .arg("5")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");
    let pid = child.id();
    let actual = read_process_start_time(pid).await.expect("start identity");
    let record = PidRecord {
        pid,
        process_start_time: format!("{actual}:different-subsecond"),
    };

    let error = process_matches_record(&record)
        .await
        .expect_err("mismatched live identity must be unverifiable");
    assert!(error.to_string().contains("identity cannot be verified"));

    child.kill().expect("stop child");
    child.wait().expect("reap child");
}

#[tokio::test]
async fn live_unverifiable_pid_record_is_preserved() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let mut child = std::process::Command::new("sleep")
        .arg("5")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");
    let record = PidRecord {
        pid: child.id(),
        process_start_time: "macos:foreign-format".to_string(),
    };
    tokio::fs::write(
        &pid_file,
        serde_json::to_vec(&record).expect("serialize pid record"),
    )
    .await
    .expect("write pid record");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file.clone(),
        /*remote_control_enabled*/ false,
    );

    let error = backend
        .running_pid()
        .await
        .expect_err("foreign live identity must fail closed");
    assert!(error.to_string().contains("identity cannot be verified"));
    assert_eq!(
        tokio::fs::read(&pid_file).await.expect("preserved record"),
        serde_json::to_vec(&record).expect("serialize expected record")
    );

    child.kill().expect("stop child");
    child.wait().expect("reap child");
}

#[tokio::test]
async fn restart_preparation_signals_do_not_stop_process() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let marker = temp_dir.path().join("hangup-received");
    let cancel_marker = temp_dir.path().join("cancel-received");
    let mut child = std::process::Command::new("/bin/sh")
        .arg("-c")
        .arg(format!(
            "trap 'touch {}' HUP; trap 'touch {}' USR1; while :; do sleep 0.1; done",
            marker.display(),
            cancel_marker.display()
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn app-server shim");
    let pid = child.id();
    let record = PidRecord {
        pid,
        process_start_time: read_process_start_time(pid).await.expect("start time"),
    };
    tokio::fs::write(
        &pid_file,
        serde_json::to_vec(&record).expect("serialize pid"),
    )
    .await
    .expect("write pid file");
    let backend = PidBackend::new(
        temp_dir.path().join("codex"),
        pid_file,
        /*remote_control_enabled*/ false,
    );

    assert_eq!(backend.running_pid().await.expect("running pid"), Some(pid));
    backend
        .signal_restart_prepared(pid, true)
        .await
        .expect("prepare");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("hangup marker");
    backend
        .signal_restart_prepared(pid, false)
        .await
        .expect("cancel");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !cancel_marker.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancel marker");
    assert!(matches!(child.try_wait(), Ok(None)));

    child.kill().expect("kill shim");
    child.wait().expect("reap shim");
}

#[test]
fn app_server_remote_control_uses_runtime_flag() {
    let backend = PidBackend::new(
        "codex".into(),
        "app-server.pid".into(),
        /*remote_control_enabled*/ true,
    );

    assert_eq!(
        backend.command_args(),
        vec!["app-server", "--remote-control", "--listen", "unix://"]
    );
}

#[test]
fn app_server_disabled_remote_control_uses_compatible_args_and_runtime_env() {
    let backend = PidBackend::new(
        "codex".into(),
        "app-server.pid".into(),
        /*remote_control_enabled*/ false,
    );

    assert_eq!(
        backend.command_args(),
        vec!["app-server", "--listen", "unix://"]
    );
    assert_eq!(
        backend.command_env(),
        Some((REMOTE_CONTROL_DISABLED_ENV_VAR, "1"))
    );
}

#[tokio::test]
async fn read_stderr_log_tail_returns_recent_complete_lines() {
    let temp_dir = TempDir::new().expect("temp dir");
    let pid_file = temp_dir.path().join("app-server.pid");
    let log_file = stderr_log_file_for_pid_file(&pid_file);
    let contents = format!("{}\nrecent error\nusage", "x".repeat(4100));
    tokio::fs::write(&log_file, contents)
        .await
        .expect("write stderr log");

    assert_eq!(
        read_stderr_log_tail(&pid_file)
            .await
            .expect("read stderr log"),
        Some(PidLogTail {
            path: log_file,
            contents: "recent error\nusage".to_string(),
        })
    );
}
