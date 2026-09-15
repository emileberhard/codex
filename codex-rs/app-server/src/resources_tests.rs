#![cfg(unix)]

use super::REQUIRED_NOFILE_SOFT_LIMIT;
use super::ensure_nofile_floor;
use pretty_assertions::assert_eq;
use std::process::Command;

const CHILD_MODE_ENV: &str = "CODEX_APP_SERVER_NOFILE_TEST_MODE";

#[test]
fn nofile_floor_is_applied_in_a_fresh_process() -> std::io::Result<()> {
    if current_nofile_limit().rlim_max < REQUIRED_NOFILE_SOFT_LIMIT {
        return Ok(());
    }
    run_child("soft-256")
}

#[test]
fn higher_nofile_soft_limit_and_hard_limit_are_preserved_in_a_fresh_process() -> std::io::Result<()>
{
    if current_nofile_limit().rlim_max < 16_384 {
        return Ok(());
    }
    run_child("soft-16384")
}

#[test]
fn an_insufficient_nofile_hard_limit_is_rejected_without_changing_limits() -> std::io::Result<()> {
    if current_nofile_limit().rlim_max < 4_096 {
        return Ok(());
    }
    run_child("hard-4096")
}

#[test]
fn nofile_floor_child() {
    let Ok(mode) = std::env::var(CHILD_MODE_ENV) else {
        return;
    };

    match mode.as_str() {
        "soft-256" => {
            let inherited_hard = current_nofile_limit().rlim_max;
            set_nofile_limit(256, inherited_hard);
            ensure_nofile_floor().expect("raise a low soft limit");
            let limit = current_nofile_limit();
            assert_eq!(limit.rlim_cur, REQUIRED_NOFILE_SOFT_LIMIT);
            assert_eq!(limit.rlim_max, inherited_hard);
        }
        "soft-16384" => {
            let inherited_hard = current_nofile_limit().rlim_max;
            set_nofile_limit(16_384, inherited_hard);
            ensure_nofile_floor().expect("preserve a sufficient soft limit");
            let limit = current_nofile_limit();
            assert_eq!(limit.rlim_cur, 16_384);
            assert_eq!(limit.rlim_max, inherited_hard);
        }
        "hard-4096" => {
            set_nofile_limit(256, 4_096);
            let error = ensure_nofile_floor().expect_err("reject an insufficient hard limit");
            assert!(error.to_string().contains("hard limit 4096"));
            let limit = current_nofile_limit();
            assert_eq!(limit.rlim_cur, 256);
            assert_eq!(limit.rlim_max, 4_096);
        }
        mode => panic!("unknown nofile child mode {mode}"),
    }
}

fn run_child(mode: &str) -> std::io::Result<()> {
    let status = Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "resources::tests::nofile_floor_child",
            "--nocapture",
        ])
        .env(CHILD_MODE_ENV, mode)
        .status()?;
    assert!(status.success(), "nofile child failed for {mode}: {status}");
    Ok(())
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

fn set_nofile_limit(soft: libc::rlim_t, hard: libc::rlim_t) {
    let limit = libc::rlimit {
        rlim_cur: soft,
        rlim_max: hard,
    };
    // SAFETY: `limit` contains valid resource-limit values and only lowers the inherited
    // soft limit or preserves the inherited hard limit in these subprocess tests.
    assert_eq!(unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) }, 0);
}
