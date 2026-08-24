use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;
#[cfg(unix)]
use std::process::Stdio;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
#[cfg(unix)]
use codex_app_server_transport::REMOTE_CONTROL_DISABLED_ENV_VAR;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncSeekExt;
#[cfg(unix)]
use tokio::process::Command;
use tokio::time::sleep;

const STOP_POLL_INTERVAL: Duration = Duration::from_millis(50);
const STOP_GRACE_PERIOD: Duration = Duration::from_secs(60);
const STOP_TIMEOUT: Duration = Duration::from_secs(70);
const START_TIMEOUT: Duration = Duration::from_secs(10);
const STDERR_LOG_TAIL_BYTES: u64 = 4096;

#[derive(Debug)]
#[cfg_attr(not(unix), allow(dead_code))]
pub(crate) struct PidBackend {
    codex_bin: PathBuf,
    pid_file: PathBuf,
    lock_file: PathBuf,
    command_kind: PidCommandKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PidRecord {
    pid: u32,
    process_start_time: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PidLogTail {
    pub(crate) path: PathBuf,
    pub(crate) contents: String,
}

impl PidLogTail {
    pub(crate) fn append_to_context(&self, context: &mut String) {
        context.push_str(&format!(
            "\n\nManaged app-server stderr ({}):",
            self.path.display()
        ));
        for line in self.contents.lines() {
            context.push_str("\n  ");
            context.push_str(line);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PidFileState {
    Missing,
    Starting,
    Running(PidRecord),
}

#[derive(Debug, Clone, Copy)]
#[cfg_attr(not(unix), allow(dead_code))]
enum PidCommandKind {
    AppServer { remote_control_enabled: bool },
}

impl PidBackend {
    pub(crate) fn new(codex_bin: PathBuf, pid_file: PathBuf, remote_control_enabled: bool) -> Self {
        let lock_file = pid_file.with_extension("pid.lock");
        Self {
            codex_bin,
            pid_file,
            lock_file,
            command_kind: PidCommandKind::AppServer {
                remote_control_enabled,
            },
        }
    }

    pub(crate) async fn is_starting_or_running(&self) -> Result<bool> {
        loop {
            match self.read_pid_file_state().await? {
                PidFileState::Missing => return Ok(false),
                PidFileState::Starting => return Ok(true),
                PidFileState::Running(record) => {
                    if self.record_is_active(&record).await? {
                        return Ok(true);
                    }
                    match self.refresh_after_stale_record(&record).await? {
                        PidFileState::Missing => return Ok(false),
                        PidFileState::Starting | PidFileState::Running(_) => continue,
                    }
                }
            }
        }
    }

    #[cfg(unix)]
    pub(crate) async fn start(&self) -> Result<Option<u32>> {
        if let Some(parent) = self.pid_file.parent() {
            fs::create_dir_all(parent)
                .await
                .with_context(|| format!("failed to create pid directory {}", parent.display()))?;
        }
        let reservation_lock = self.acquire_reservation_lock().await?;
        let _pid_file = loop {
            match fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&self.pid_file)
                .await
            {
                Ok(pid_file) => break pid_file,
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    match self.read_pid_file_state_with_lock_held().await? {
                        PidFileState::Missing => continue,
                        PidFileState::Running(record) => {
                            if self.record_is_active(&record).await? {
                                return Ok(None);
                            }
                            let _ = fs::remove_file(&self.pid_file).await;
                            continue;
                        }
                        PidFileState::Starting => {
                            unreachable!("lock holder cannot observe starting")
                        }
                    }
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!("failed to reserve pid file {}", self.pid_file.display())
                    });
                }
            }
        };
        let mut command = Command::new(&self.codex_bin);
        let stderr_log = match self.open_stderr_log().await {
            Ok(stderr_log) => stderr_log,
            Err(err) => {
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err);
            }
        };
        command
            .args(self.command_args())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_log.into_std().await));
        if let Some((key, value)) = self.command_env() {
            command.env(key, value);
        }

        #[cfg(unix)]
        {
            unsafe {
                command.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }

        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(err) => {
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err).with_context(|| {
                    format!(
                        "failed to spawn detached app-server process using {}",
                        self.codex_bin.display()
                    )
                });
            }
        };
        let pid = child
            .id()
            .context("spawned app-server process has no pid")?;
        let record = match read_process_start_time(pid).await {
            Ok(process_start_time) => PidRecord {
                pid,
                process_start_time,
            },
            Err(err) => {
                terminate_spawned_child(&mut child).await;
                let mut context =
                    format!("failed to record pid-managed app-server process {pid} startup");
                super::append_stderr_log_tail_context(&self.pid_file, &mut context).await;
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(err).context(context);
            }
        };
        let contents = match serde_json::to_vec(&record) {
            Ok(contents) => contents,
            Err(error) => {
                terminate_spawned_child(&mut child).await;
                let _ = fs::remove_file(&self.pid_file).await;
                return Err(error).context("failed to serialize pid record");
            }
        };
        let temp_pid_file = self.pid_file.with_extension("pid.tmp");
        if let Err(err) = fs::write(&temp_pid_file, &contents).await {
            terminate_spawned_child(&mut child).await;
            let _ = fs::remove_file(&self.pid_file).await;
            return Err(err).with_context(|| {
                format!("failed to write pid temp file {}", temp_pid_file.display())
            });
        }
        if let Err(err) = fs::rename(&temp_pid_file, &self.pid_file).await {
            terminate_spawned_child(&mut child).await;
            let _ = fs::remove_file(&temp_pid_file).await;
            let _ = fs::remove_file(&self.pid_file).await;
            return Err(err).with_context(|| {
                format!("failed to publish pid file {}", self.pid_file.display())
            });
        }
        tokio::spawn(async move {
            let _ = child.wait().await;
        });
        drop(reservation_lock);
        Ok(Some(pid))
    }

    #[cfg(not(unix))]
    pub(crate) async fn start(&self) -> Result<Option<u32>> {
        bail!("pid-managed app-server startup is unsupported on this platform")
    }

    pub(crate) async fn stop(&self) -> Result<()> {
        loop {
            let Some(record) = self.wait_for_pid_start().await? else {
                return Ok(());
            };
            if !self.record_is_active(&record).await? {
                match self.refresh_after_stale_record(&record).await? {
                    PidFileState::Missing => return Ok(()),
                    PidFileState::Starting | PidFileState::Running(_) => continue,
                }
            }

            let pid = record.pid;
            self.terminate_process(pid)?;
            let started_at = tokio::time::Instant::now();
            let deadline = tokio::time::Instant::now() + STOP_TIMEOUT;
            let mut forced = false;
            loop {
                #[cfg(unix)]
                if let Ok(raw_pid) = libc::pid_t::try_from(pid)
                    && raw_pid > 0
                {
                    // A previous daemon invocation may have started this child; reap it if exited.
                    unsafe { libc::waitpid(raw_pid, std::ptr::null_mut(), libc::WNOHANG) };
                }
                if !self.record_is_active(&record).await? {
                    match self.refresh_after_stale_record(&record).await? {
                        PidFileState::Missing => return Ok(()),
                        PidFileState::Starting | PidFileState::Running(_) => break,
                    }
                }
                if tokio::time::Instant::now() >= deadline {
                    break;
                }
                if !forced && started_at.elapsed() >= STOP_GRACE_PERIOD {
                    self.force_terminate_process(pid)?;
                    forced = true;
                }
                sleep(STOP_POLL_INTERVAL).await;
            }

            if self.record_is_active(&record).await? {
                bail!("timed out waiting for pid-managed app server {pid} to stop");
            }
        }
    }

    pub(crate) async fn running_pid(&self) -> Result<Option<u32>> {
        loop {
            let Some(record) = self.wait_for_pid_start().await? else {
                return Ok(None);
            };
            if !self.record_is_active(&record).await? {
                match self.refresh_after_stale_record(&record).await? {
                    PidFileState::Missing => return Ok(None),
                    PidFileState::Starting | PidFileState::Running(_) => continue,
                }
            }
            return Ok(Some(record.pid));
        }
    }

    /// Ask the exact running app-server process to enter or leave its
    /// restart-preparation fence. Capability probing happens before this call.
    #[cfg(unix)]
    pub(crate) async fn signal_restart_prepared(
        &self,
        expected_pid: u32,
        prepared: bool,
    ) -> Result<()> {
        let Some(pid) = self.running_pid().await? else {
            bail!("pid-managed app server disappeared before restart preparation")
        };
        if pid != expected_pid {
            bail!(
                "pid-managed app server changed from pid {expected_pid} to {pid} during restart preparation"
            );
        }
        send_process_signal(
            pid,
            if prepared {
                libc::SIGHUP
            } else {
                libc::SIGUSR1
            },
            if prepared {
                "prepare restart for"
            } else {
                "cancel prepared restart for"
            },
        )
    }

    #[cfg(not(unix))]
    pub(crate) async fn signal_restart_prepared(
        &self,
        _expected_pid: u32,
        _prepared: bool,
    ) -> Result<()> {
        bail!("pid-managed app-server restart preparation is unsupported on this platform")
    }

    async fn wait_for_pid_start(&self) -> Result<Option<PidRecord>> {
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            match self.read_pid_file_state().await? {
                PidFileState::Missing => return Ok(None),
                PidFileState::Running(record) => return Ok(Some(record)),
                PidFileState::Starting if tokio::time::Instant::now() < deadline => {
                    sleep(STOP_POLL_INTERVAL).await;
                }
                PidFileState::Starting => {
                    bail!(
                        "timed out waiting for pid reservation in {} to finish initializing",
                        self.pid_file.display()
                    );
                }
            }
        }
    }

    async fn read_pid_file_state(&self) -> Result<PidFileState> {
        let contents = match fs::read_to_string(&self.pid_file).await {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return if reservation_lock_is_active(&self.lock_file).await? {
                    Ok(PidFileState::Starting)
                } else {
                    Ok(PidFileState::Missing)
                };
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read pid file {}", self.pid_file.display())
                });
            }
        };
        if contents.trim().is_empty() {
            match inspect_empty_pid_reservation(&self.pid_file, &self.lock_file).await? {
                EmptyPidReservation::Active => {
                    return Ok(PidFileState::Starting);
                }
                EmptyPidReservation::Stale => {
                    return Ok(PidFileState::Missing);
                }
                EmptyPidReservation::Record(record) => return Ok(PidFileState::Running(record)),
            }
        }
        let record = serde_json::from_str(&contents)
            .with_context(|| format!("invalid pid file contents in {}", self.pid_file.display()))?;
        Ok(PidFileState::Running(record))
    }

    async fn read_pid_file_state_with_lock_held(&self) -> Result<PidFileState> {
        let contents = match fs::read_to_string(&self.pid_file).await {
            Ok(contents) => contents,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(PidFileState::Missing);
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read pid file {}", self.pid_file.display())
                });
            }
        };
        if contents.trim().is_empty() {
            let _ = fs::remove_file(&self.pid_file).await;
            return Ok(PidFileState::Missing);
        }
        let record = serde_json::from_str(&contents)
            .with_context(|| format!("invalid pid file contents in {}", self.pid_file.display()))?;
        Ok(PidFileState::Running(record))
    }

    async fn refresh_after_stale_record(&self, expected: &PidRecord) -> Result<PidFileState> {
        let reservation_lock = self.acquire_reservation_lock().await?;
        let state = match self.read_pid_file_state_with_lock_held().await? {
            PidFileState::Running(record) if record == *expected => {
                let _ = fs::remove_file(&self.pid_file).await;
                PidFileState::Missing
            }
            state => state,
        };
        drop(reservation_lock);
        Ok(state)
    }

    async fn acquire_reservation_lock(&self) -> Result<fs::File> {
        let reservation_lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.lock_file)
            .await
            .with_context(|| {
                format!("failed to open pid lock file {}", self.lock_file.display())
            })?;
        let lock_deadline = tokio::time::Instant::now() + START_TIMEOUT;
        while !try_lock_file(&reservation_lock)? {
            if tokio::time::Instant::now() >= lock_deadline {
                bail!(
                    "timed out waiting for pid lock {}",
                    self.lock_file.display()
                );
            }
            sleep(STOP_POLL_INTERVAL).await;
        }
        Ok(reservation_lock)
    }

    #[cfg(unix)]
    async fn open_stderr_log(&self) -> Result<fs::File> {
        let stderr_log_file = stderr_log_file_for_pid_file(&self.pid_file);
        fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stderr_log_file)
            .await
            .with_context(|| {
                format!(
                    "failed to open stderr log for pid-managed app server {}",
                    stderr_log_file.display()
                )
            })
    }

    #[cfg(unix)]
    fn command_args(&self) -> Vec<&'static str> {
        match self.command_kind {
            PidCommandKind::AppServer {
                remote_control_enabled: true,
            } => vec!["app-server", "--remote-control", "--listen", "unix://"],
            PidCommandKind::AppServer {
                remote_control_enabled: false,
            } => vec!["app-server", "--listen", "unix://"],
        }
    }

    #[cfg(unix)]
    fn command_env(&self) -> Option<(&'static str, &'static str)> {
        match self.command_kind {
            PidCommandKind::AppServer {
                remote_control_enabled: false,
            } => Some((REMOTE_CONTROL_DISABLED_ENV_VAR, "1")),
            PidCommandKind::AppServer {
                remote_control_enabled: true,
            } => None,
        }
    }

    fn terminate_process(&self, pid: u32) -> Result<()> {
        terminate_process_group(pid)
    }

    fn force_terminate_process(&self, pid: u32) -> Result<()> {
        force_terminate_process_group(pid)
    }

    async fn record_is_active(&self, record: &PidRecord) -> Result<bool> {
        process_matches_record(record).await
    }
}

pub(crate) async fn read_stderr_log_tail(pid_file: &Path) -> Result<Option<PidLogTail>> {
    let path = stderr_log_file_for_pid_file(pid_file);
    let Some(contents) = read_log_tail(&path, STDERR_LOG_TAIL_BYTES).await? else {
        return Ok(None);
    };
    Ok(Some(PidLogTail { path, contents }))
}

fn stderr_log_file_for_pid_file(pid_file: &Path) -> PathBuf {
    pid_file.with_extension("stderr.log")
}

async fn read_log_tail(path: &Path, byte_limit: u64) -> Result<Option<String>> {
    let mut file = match fs::File::open(path).await {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to open stderr log {}", path.display()));
        }
    };
    let len = file
        .metadata()
        .await
        .with_context(|| format!("failed to inspect stderr log {}", path.display()))?
        .len();
    if len == 0 {
        return Ok(None);
    }

    let start = len.saturating_sub(byte_limit);
    file.seek(SeekFrom::Start(start))
        .await
        .with_context(|| format!("failed to seek stderr log {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .await
        .with_context(|| format!("failed to read stderr log {}", path.display()))?;
    if start > 0
        && let Some(newline_index) = bytes.iter().position(|byte| *byte == b'\n')
    {
        bytes.drain(..=newline_index);
    }
    let contents = String::from_utf8_lossy(&bytes).trim_end().to_string();
    if contents.is_empty() {
        return Ok(None);
    }
    Ok(Some(contents))
}

#[cfg(unix)]
fn process_exists(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(pid, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(unix)]
fn terminate_process_group(pid: u32) -> Result<()> {
    send_process_signal(pid, libc::SIGTERM, "terminate")?;
    send_process_group_signal(pid, libc::SIGTERM, "terminate descendants")
}

#[cfg(unix)]
fn send_process_signal(pid: u32, signal: libc::c_int, action: &str) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed app server pid {pid} is out of range"))?;
    let result = unsafe { libc::kill(raw_pid, signal) };
    if result == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err).with_context(|| format!("failed to {action} pid-managed app server {pid}"))
}

#[cfg(unix)]
fn force_terminate_process_group(pid: u32) -> Result<()> {
    send_process_signal(pid, libc::SIGKILL, "force terminate")?;
    send_process_group_signal(pid, libc::SIGKILL, "force terminate descendants")
}

#[cfg(unix)]
fn send_process_group_signal(pid: u32, signal: libc::c_int, action: &str) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed process pid {pid} is out of range"))?;
    let result = unsafe { libc::kill(-raw_pid, signal) };
    if result == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::ESRCH) {
        return Ok(());
    }
    Err(err).with_context(|| format!("failed to {action} pid-managed process group {pid}"))
}

#[cfg(not(unix))]
fn terminate_process_group(_pid: u32) -> Result<()> {
    bail!("pid-managed app-server shutdown is unsupported on this platform")
}

#[cfg(not(unix))]
fn force_terminate_process_group(_pid: u32) -> Result<()> {
    bail!("pid-managed app-server shutdown is unsupported on this platform")
}

#[cfg(unix)]
async fn process_matches_record(record: &PidRecord) -> Result<bool> {
    if !process_exists(record.pid) {
        return Ok(false);
    }

    match read_process_state(record.pid).await {
        Ok(state) if state.starts_with('Z') => {
            reap_child_if_owned(record.pid)?;
            return Ok(false);
        }
        Ok(_) => {}
        Err(_err) if !process_exists(record.pid) => return Ok(false),
        Err(err) => return Err(err),
    }

    match read_process_start_time(record.pid).await {
        Ok(start_time) => Ok(start_time == record.process_start_time),
        Err(_err) if !process_exists(record.pid) => Ok(false),
        Err(err) => Err(err),
    }
}

#[cfg(unix)]
fn reap_child_if_owned(pid: u32) -> Result<()> {
    let raw_pid = libc::pid_t::try_from(pid)
        .with_context(|| format!("pid-managed app server pid {pid} is out of range"))?;
    let mut status = 0;
    let result = unsafe { libc::waitpid(raw_pid, &mut status, libc::WNOHANG) };
    if result == raw_pid || result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ECHILD) {
        return Ok(());
    }
    Err(error).with_context(|| format!("failed to reap pid-managed app server {pid}"))
}

#[cfg(unix)]
async fn terminate_spawned_child(child: &mut tokio::process::Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}

#[cfg(unix)]
async fn read_process_state(pid: u32) -> Result<String> {
    let output = Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .await
        .with_context(|| format!("failed to inspect process state for pid {pid}"))?;
    if !output.status.success() {
        bail!("failed to inspect process state for pid {pid}");
    }
    let state = String::from_utf8(output.stdout)
        .context("process state output was not valid UTF-8")?
        .trim()
        .to_string();
    if state.is_empty() {
        bail!("process state was empty for pid {pid}");
    }
    Ok(state)
}

#[cfg(not(unix))]
async fn process_matches_record(_record: &PidRecord) -> Result<bool> {
    Ok(false)
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(not(unix), allow(dead_code))]
enum EmptyPidReservation {
    Active,
    Stale,
    Record(PidRecord),
}

#[cfg(unix)]
fn try_lock_file(file: &fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }

    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
        return Ok(false);
    }
    Err(err).context("failed to lock pid reservation")
}

#[cfg(not(unix))]
fn try_lock_file(_file: &fs::File) -> Result<bool> {
    bail!("pid-managed app-server startup is unsupported on this platform")
}

#[cfg(unix)]
async fn reservation_lock_is_active(path: &Path) -> Result<bool> {
    let file = match fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .await
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(false);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect pid lock file {}", path.display()));
        }
    };
    Ok(!try_lock_file(&file)?)
}

#[cfg(not(unix))]
async fn reservation_lock_is_active(_path: &Path) -> Result<bool> {
    Ok(false)
}

#[cfg(unix)]
async fn inspect_empty_pid_reservation(
    pid_path: &Path,
    lock_path: &Path,
) -> Result<EmptyPidReservation> {
    let file = match fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)
        .await
    {
        Ok(file) => file,
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to inspect pid lock file {}", lock_path.display())
            });
        }
    };
    if !try_lock_file(&file)? {
        return Ok(EmptyPidReservation::Active);
    }

    let contents = match fs::read_to_string(pid_path).await {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(EmptyPidReservation::Stale);
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to reread pid file {}", pid_path.display()));
        }
    };
    if contents.trim().is_empty() {
        let _ = fs::remove_file(pid_path).await;
        return Ok(EmptyPidReservation::Stale);
    }

    let record = serde_json::from_str(&contents)
        .with_context(|| format!("invalid pid file contents in {}", pid_path.display()))?;
    Ok(EmptyPidReservation::Record(record))
}

#[cfg(not(unix))]
async fn inspect_empty_pid_reservation(
    _pid_path: &Path,
    _lock_path: &Path,
) -> Result<EmptyPidReservation> {
    Ok(EmptyPidReservation::Stale)
}

#[cfg(target_os = "macos")]
async fn read_process_start_time(pid: u32) -> Result<String> {
    const PROC_PIDTBSDINFO: libc::c_int = 3;

    #[repr(C)]
    #[derive(Default)]
    struct ProcBsdInfo {
        pbi_flags: u32,
        pbi_status: u32,
        pbi_xstatus: u32,
        pbi_pid: u32,
        pbi_ppid: u32,
        pbi_uid: u32,
        pbi_gid: u32,
        pbi_ruid: u32,
        pbi_rgid: u32,
        pbi_svuid: u32,
        pbi_svgid: u32,
        rfu_1: u32,
        pbi_comm: [u8; 16],
        pbi_name: [u8; 32],
        pbi_nfiles: u32,
        pbi_pgid: u32,
        pbi_pjobc: u32,
        e_tdev: u32,
        e_tpgid: u32,
        pbi_nice: i32,
        pbi_start_tvsec: u64,
        pbi_start_tvusec: u64,
    }

    unsafe extern "C" {
        fn proc_pidinfo(
            pid: libc::c_int,
            flavor: libc::c_int,
            arg: u64,
            buffer: *mut libc::c_void,
            buffersize: libc::c_int,
        ) -> libc::c_int;
    }

    let raw_pid = libc::c_int::try_from(pid)
        .with_context(|| format!("pid-managed app server pid {pid} is out of range"))?;
    let mut info = ProcBsdInfo::default();
    let expected_size = std::mem::size_of::<ProcBsdInfo>();
    let actual_size = unsafe {
        proc_pidinfo(
            raw_pid,
            PROC_PIDTBSDINFO,
            0,
            (&mut info as *mut ProcBsdInfo).cast(),
            libc::c_int::try_from(expected_size).context("proc_bsdinfo size is out of range")?,
        )
    };
    if usize::try_from(actual_size).ok() != Some(expected_size) {
        bail!("failed to read kernel start time for pid-managed app server {pid}");
    }
    Ok(format!(
        "macos:{}:{}",
        info.pbi_start_tvsec, info.pbi_start_tvusec
    ))
}

#[cfg(target_os = "linux")]
async fn read_process_start_time(pid: u32) -> Result<String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
        .await
        .with_context(|| format!("failed to read kernel start time for pid {pid}"))?;
    let fields = stat
        .rsplit_once(") ")
        .map(|(_, fields)| fields)
        .ok_or_else(|| anyhow::anyhow!("invalid /proc/{pid}/stat process identity"))?;
    let start_ticks = fields
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| anyhow::anyhow!("/proc/{pid}/stat omitted process start ticks"))?;
    Ok(format!("linux:{start_ticks}"))
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "linux"))))]
async fn read_process_start_time(pid: u32) -> Result<String> {
    bail!("kernel process identity is unsupported for pid-managed app server {pid}")
}

#[cfg(all(test, unix))]
#[path = "pid_tests.rs"]
mod tests;
