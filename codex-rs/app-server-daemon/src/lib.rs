mod backend;
mod client;
mod managed_install;
mod remote_control_client;
mod settings;
mod stdio_proxy;

#[cfg(all(test, unix))]
#[path = "startup_tests.rs"]
mod startup_tests;

use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
pub use backend::BackendKind;
use backend::BackendPaths;
use codex_app_server_protocol::RemoteControlConnectionStatus;
use codex_app_server_protocol::RemoteControlPairingStartResponse;
use codex_app_server_transport::app_server_control_socket_path;
use codex_utils_home_dir::find_codex_home;
use managed_install::managed_codex_bin;
#[cfg(unix)]
use managed_install::managed_codex_version;
use serde::Serialize;
use settings::DaemonSettings;
pub use stdio_proxy::proxy_stdio;
use tokio::time::sleep;

const START_POLL_INTERVAL: Duration = Duration::from_millis(50);
const START_TIMEOUT: Duration = Duration::from_secs(10);
const OPERATION_LOCK_TIMEOUT: Duration = Duration::from_secs(75);
const PID_FILE_NAME: &str = "app-server.pid";
const OPERATION_LOCK_FILE_NAME: &str = "daemon.lock";
const SETTINGS_FILE_NAME: &str = "settings.json";
const STATE_DIR_NAME: &str = "app-server-daemon";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LifecycleCommand {
    Start,
    Restart,
    PrepareRestart,
    CancelRestart,
    Stop,
    Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LifecycleStatus {
    AlreadyRunning,
    Started,
    Restarted,
    Stopped,
    NotRunning,
    Running,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleOutput {
    pub status: LifecycleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<BackendKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub managed_codex_path: PathBuf,
    pub managed_codex_version: Option<String>,
    pub socket_path: PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cli_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_server_version: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BootstrapOptions {
    pub remote_control_enabled: bool,
}

/// Passively probes an existing app-server socket and returns its reported
/// app-server version.
pub async fn probe_app_server_version(socket_path: &Path) -> Result<String> {
    Ok(client::probe(socket_path).await?.app_server_version)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum BootstrapStatus {
    Bootstrapped,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapOutput {
    pub status: BootstrapStatus,
    pub backend: BackendKind,
    pub auto_update_enabled: bool,
    pub remote_control_enabled: bool,
    pub managed_codex_path: PathBuf,
    pub managed_codex_version: Option<String>,
    pub socket_path: PathBuf,
    pub cli_version: String,
    pub app_server_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum RemoteControlStartOutput {
    Bootstrap(BootstrapOutput),
    Start(LifecycleOutput),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteControlReadyStatus {
    pub status: RemoteControlConnectionStatus,
    pub server_name: String,
    pub environment_id: Option<String>,
    pub timed_out: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteControlReadyOutput {
    pub daemon: RemoteControlStartOutput,
    pub remote_control: RemoteControlReadyStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteControlMode {
    Enabled,
    Disabled,
}

impl RemoteControlMode {
    fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoteControlStatus {
    Enabled,
    Disabled,
    AlreadyEnabled,
    AlreadyDisabled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteControlOutput {
    pub status: RemoteControlStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub backend: Option<BackendKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    pub remote_control_enabled: bool,
    pub socket_path: PathBuf,
    pub cli_version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_server_version: Option<String>,
}

pub async fn run(command: LifecycleCommand) -> Result<LifecycleOutput> {
    ensure_supported_platform()?;
    Daemon::from_environment()?.run(command).await
}

pub async fn bootstrap(options: BootstrapOptions) -> Result<BootstrapOutput> {
    ensure_supported_platform()?;
    Daemon::from_environment()?.bootstrap(options).await
}

pub async fn ensure_remote_control_ready() -> Result<RemoteControlReadyOutput> {
    ensure_supported_platform()?;
    Daemon::from_environment()?
        .ensure_remote_control_ready()
        .await
}

pub async fn enable_remote_control_on_socket(
    socket_path: &Path,
    connect_timeout: Duration,
    connect_retry_delay: Duration,
) -> Result<RemoteControlReadyStatus> {
    ensure_supported_platform()?;
    remote_control_client::enable_remote_control_with_connect_retry(
        socket_path,
        connect_timeout,
        connect_retry_delay,
    )
    .await
}

/// Starts a manual pairing session through an already-running daemon app-server.
pub async fn start_remote_control_pairing() -> Result<RemoteControlPairingStartResponse> {
    ensure_supported_platform()?;
    let daemon = Daemon::from_environment()?;
    remote_control_client::start_pairing(&daemon.socket_path).await
}

pub async fn set_remote_control(mode: RemoteControlMode) -> Result<RemoteControlOutput> {
    ensure_supported_platform()?;
    Daemon::from_environment()?.set_remote_control(mode).await
}

#[cfg(unix)]
fn ensure_supported_platform() -> Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn ensure_supported_platform() -> Result<()> {
    Err(anyhow!(
        "codex app-server daemon lifecycle is only supported on Unix platforms"
    ))
}

struct Daemon {
    socket_path: PathBuf,
    pid_file: PathBuf,
    operation_lock_file: PathBuf,
    settings_file: PathBuf,
    managed_codex_bin: PathBuf,
}

impl Daemon {
    fn from_environment() -> Result<Self> {
        let codex_home = find_codex_home().context("failed to resolve CODEX_HOME")?;
        let socket_path = app_server_control_socket_path(codex_home.as_path())?
            .as_path()
            .to_path_buf();
        let state_dir = codex_home.as_path().join(STATE_DIR_NAME);
        Ok(Self {
            socket_path,
            pid_file: state_dir.join(PID_FILE_NAME),
            operation_lock_file: state_dir.join(OPERATION_LOCK_FILE_NAME),
            settings_file: state_dir.join(SETTINGS_FILE_NAME),
            managed_codex_bin: managed_codex_bin(codex_home.as_path()),
        })
    }

    async fn run(&self, command: LifecycleCommand) -> Result<LifecycleOutput> {
        match command {
            LifecycleCommand::Start => {
                let _operation_lock = self.acquire_operation_lock().await?;
                self.start().await
            }
            LifecycleCommand::Restart => {
                let _operation_lock = self.acquire_operation_lock().await?;
                self.restart().await
            }
            LifecycleCommand::PrepareRestart => {
                let _operation_lock = self.acquire_operation_lock().await?;
                self.set_restart_prepared(true).await
            }
            LifecycleCommand::CancelRestart => {
                let _operation_lock = self.acquire_operation_lock().await?;
                self.set_restart_prepared(false).await
            }
            LifecycleCommand::Stop => {
                let _operation_lock = self.acquire_operation_lock().await?;
                self.stop().await
            }
            LifecycleCommand::Version => self.version().await,
        }
    }

    async fn start(&self) -> Result<LifecycleOutput> {
        let settings = self.load_settings().await?;
        if let Ok(info) = client::probe(&self.socket_path).await {
            let Some(backend) = self.running_backend_instance(&settings).await? else {
                return Err(anyhow!(
                    "app server is running but is not managed by codex app-server daemon"
                ));
            };
            let pid = self.verify_probe_owner(&backend, info.process_id).await?;
            return Ok(self
                .output(
                    LifecycleStatus::AlreadyRunning,
                    Some(BackendKind::Pid),
                    Some(pid),
                    Some(info.app_server_version),
                )
                .await);
        }

        if let Some(backend) = self.running_backend_instance(&settings).await? {
            let pid = backend
                .running_pid()
                .await?
                .ok_or_else(|| anyhow!("managed app server disappeared before readiness probe"))?;
            let info = self.wait_until_ready(pid).await?;
            return Ok(self
                .output(
                    LifecycleStatus::AlreadyRunning,
                    Some(BackendKind::Pid),
                    Some(pid),
                    Some(info.app_server_version),
                )
                .await);
        }

        // The transport owns stale-path removal under its startup lock. Permit
        // that recovery only after a refused connection to an actual socket;
        // failed protocol probes alone do not establish that nobody is listening.
        let stale_socket = matches!(
            tokio::time::timeout(
                client::CONTROL_SOCKET_RESPONSE_TIMEOUT,
                codex_uds::UnixStream::connect(&self.socket_path),
            )
            .await,
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused
        ) && codex_uds::is_stale_socket_path(&self.socket_path)
            .await
            .context("failed to inspect refused app-server socket")?;
        if !stale_socket {
            self.ensure_app_server_absent().await?;
        }
        self.ensure_managed_codex_bin()?;
        let pid = self.start_managed_backend(&settings).await?;
        let expected_pid =
            pid.ok_or_else(|| anyhow!("managed app server start omitted its pid"))?;
        let info = self.wait_until_ready(expected_pid).await?;
        Ok(self
            .output(
                LifecycleStatus::Started,
                Some(BackendKind::Pid),
                pid,
                Some(info.app_server_version),
            )
            .await)
    }

    async fn restart(&self) -> Result<LifecycleOutput> {
        let settings = self.load_settings().await?;
        if let Ok(info) = client::probe(&self.socket_path).await {
            let Some(backend) = self.running_backend_instance(&settings).await? else {
                return Err(anyhow!(
                    "app server is running but is not managed by codex app-server daemon"
                ));
            };
            self.verify_probe_owner(&backend, info.process_id).await?;
        }

        self.ensure_managed_codex_bin()?;
        if let Some(backend) = self.running_backend_instance(&settings).await? {
            backend.stop().await?;
        }

        self.ensure_app_server_absent().await?;
        let pid = self.start_managed_backend(&settings).await?;
        let expected_pid =
            pid.ok_or_else(|| anyhow!("managed app server start omitted its pid"))?;
        let info = self.wait_until_ready(expected_pid).await?;
        Ok(self
            .output(
                LifecycleStatus::Restarted,
                Some(BackendKind::Pid),
                pid,
                Some(info.app_server_version),
            )
            .await)
    }

    async fn set_restart_prepared(&self, prepared: bool) -> Result<LifecycleOutput> {
        let settings = self.load_settings().await?;
        let Some(backend) = self.running_backend_instance(&settings).await? else {
            self.ensure_app_server_absent().await?;
            return Ok(self
                .output(
                    LifecycleStatus::NotRunning,
                    /*backend*/ None,
                    /*pid*/ None,
                    /*app_server_version*/ None,
                )
                .await);
        };

        let pid = backend
            .running_pid()
            .await?
            .ok_or_else(|| anyhow!("managed app server disappeared before restart preparation"))?;
        let initial = client::probe_new_work_admission_fenced(&self.socket_path)
            .await
            .context("managed app server does not support verified restart admission fencing")?;
        self.verify_probe_owner(&backend, initial.process_id)
            .await?;
        let desired = if prepared {
            initial.closed && initial.fenced
        } else {
            !initial.closed && !initial.fenced
        };
        if desired {
            return Ok(self
                .output(
                    LifecycleStatus::Running,
                    Some(BackendKind::Pid),
                    Some(pid),
                    Some(initial.app_server_version),
                )
                .await);
        }

        backend.signal_restart_prepared(pid, prepared).await?;
        let info = match self
            .wait_for_restart_preparation(&backend, pid, prepared)
            .await
        {
            Ok(info) => info,
            Err(error) if prepared => {
                let cancellation = async {
                    backend.signal_restart_prepared(pid, false).await?;
                    self.wait_for_restart_preparation(&backend, pid, false)
                        .await
                }
                .await;
                return match cancellation {
                    Ok(_) => Err(error).context(
                        "restart preparation was not acknowledged; admission was reopened",
                    ),
                    Err(cancel_error) => Err(error).context(format!(
                        "restart preparation was not acknowledged and admission could not be reopened: {cancel_error:#}"
                    )),
                };
            }
            Err(error) => return Err(error),
        };
        Ok(self
            .output(
                LifecycleStatus::Running,
                Some(BackendKind::Pid),
                Some(pid),
                Some(info.app_server_version),
            )
            .await)
    }

    async fn wait_for_restart_preparation(
        &self,
        backend: &backend::PidBackend,
        expected_pid: u32,
        prepared: bool,
    ) -> Result<client::WorkAdmissionProbe> {
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            match client::probe_new_work_admission_fenced(&self.socket_path).await {
                Ok(info)
                    if info.process_id == expected_pid
                        && ((prepared && info.closed && info.fenced)
                            || (!prepared && !info.closed && !info.fenced)) =>
                {
                    return Ok(info);
                }
                Ok(info) if info.process_id != expected_pid => {
                    return Err(anyhow!(
                        "app-server socket owner changed from pid {expected_pid} to {} while waiting for restart preparation",
                        info.process_id
                    ));
                }
                Ok(_) if tokio::time::Instant::now() < deadline => {}
                Err(error) => {
                    match backend.running_pid().await? {
                        Some(pid) if pid == expected_pid => {}
                        Some(pid) => {
                            return Err(anyhow!(
                                "managed app server changed from pid {expected_pid} to {pid} while waiting for restart preparation"
                            ));
                        }
                        None => {
                            return Err(error).context(format!(
                                "managed app server pid {expected_pid} disappeared while waiting for restart preparation"
                            ));
                        }
                    }
                    if tokio::time::Instant::now() >= deadline {
                        return Err(error).context("restart admission probe timed out");
                    }
                }
                Ok(_) => {
                    return Err(anyhow!(
                        "app server did not acknowledge restart admission {}",
                        if prepared { "fencing" } else { "reopening" }
                    ));
                }
            }
            sleep(START_POLL_INTERVAL).await;
        }
    }

    async fn ensure_app_server_absent(&self) -> Result<()> {
        match client::probe(&self.socket_path).await {
            Ok(_) => Err(anyhow!(
                "app server is running but is not managed by codex app-server daemon"
            )),
            Err(probe_error) => match self.socket_path.try_exists() {
                Ok(false) => Ok(()),
                Ok(true) => Err(probe_error).with_context(|| {
                    format!(
                        "app-server socket {} exists but its owner could not be verified",
                        self.socket_path.display()
                    )
                }),
                Err(stat_error) => Err(stat_error).with_context(|| {
                    format!(
                        "failed to verify whether app-server socket {} exists",
                        self.socket_path.display()
                    )
                }),
            },
        }
    }

    async fn stop(&self) -> Result<LifecycleOutput> {
        let settings = self.load_settings().await?;
        if let Some(backend) = self.running_backend_instance(&settings).await? {
            backend.stop().await?;
            self.ensure_app_server_absent().await?;
            return Ok(self
                .output(
                    LifecycleStatus::Stopped,
                    Some(BackendKind::Pid),
                    /*pid*/ None,
                    /*app_server_version*/ None,
                )
                .await);
        }

        self.ensure_app_server_absent().await?;

        Ok(self
            .output(
                LifecycleStatus::NotRunning,
                /*backend*/ None,
                /*pid*/ None,
                /*app_server_version*/ None,
            )
            .await)
    }

    async fn version(&self) -> Result<LifecycleOutput> {
        let settings = self.load_settings().await?;
        let info = client::probe(&self.socket_path).await?;
        let Some(backend) = self.running_backend_instance(&settings).await? else {
            return Err(anyhow!(
                "app server is running but is not managed by codex app-server daemon"
            ));
        };
        let pid = self.verify_probe_owner(&backend, info.process_id).await?;
        Ok(self
            .output(
                LifecycleStatus::Running,
                Some(BackendKind::Pid),
                Some(pid),
                Some(info.app_server_version),
            )
            .await)
    }

    async fn wait_until_ready(&self, expected_pid: u32) -> Result<client::ProbeInfo> {
        let deadline = tokio::time::Instant::now() + START_TIMEOUT;
        loop {
            match client::probe(&self.socket_path).await {
                Ok(info) if info.process_id == expected_pid => return Ok(info),
                Ok(info) => {
                    verify_process_identity(expected_pid, info.process_id)?;
                    return Ok(info);
                }
                Err(err) if tokio::time::Instant::now() < deadline => {
                    let _ = err;
                    sleep(START_POLL_INTERVAL).await;
                }
                Err(err) => {
                    let context = self.app_server_not_ready_context().await;
                    return Err(err).context(context);
                }
            }
        }
    }

    async fn verify_probe_owner(
        &self,
        backend: &backend::PidBackend,
        observed_pid: u32,
    ) -> Result<u32> {
        let pid = backend
            .running_pid()
            .await?
            .ok_or_else(|| anyhow!("managed app server disappeared during identity probe"))?;
        verify_process_identity(pid, observed_pid)?;
        Ok(pid)
    }

    async fn app_server_not_ready_context(&self) -> String {
        let mut context = format!(
            "app server did not become ready on {}",
            self.socket_path.display()
        );
        self.append_daemon_app_server_context(&mut context).await;
        backend::append_stderr_log_tail_context(&self.pid_file, &mut context).await;
        context
    }

    async fn append_daemon_app_server_context(&self, context: &mut String) {
        let managed_codex_version = self
            .managed_codex_version_best_effort()
            .await
            .unwrap_or_else(|| "unknown".to_string());
        context.push_str(&format!(
            "\n\nDaemon used app-server:\n  path: {}\n  version: {managed_codex_version}",
            self.managed_codex_bin.display()
        ));
    }

    async fn bootstrap(&self, options: BootstrapOptions) -> Result<BootstrapOutput> {
        let _operation_lock = self.acquire_operation_lock().await?;
        self.bootstrap_locked(options).await
    }

    async fn ensure_remote_control_started(&self) -> Result<RemoteControlStartOutput> {
        let _operation_lock = self.acquire_operation_lock().await?;
        let settings = self.load_settings().await?;
        if self.is_bootstrapped(&settings).await? {
            let _ = self
                .set_remote_control_locked(RemoteControlMode::Enabled)
                .await?;
            let output = self.start().await?;
            return Ok(RemoteControlStartOutput::Start(output));
        }

        let output = self
            .bootstrap_locked(BootstrapOptions {
                remote_control_enabled: true,
            })
            .await?;
        Ok(RemoteControlStartOutput::Bootstrap(output))
    }

    async fn ensure_remote_control_ready(&self) -> Result<RemoteControlReadyOutput> {
        let daemon = self.ensure_remote_control_started().await?;
        let remote_control =
            remote_control_client::enable_remote_control(&self.socket_path).await?;
        Ok(RemoteControlReadyOutput {
            daemon,
            remote_control,
        })
    }

    async fn set_remote_control(&self, mode: RemoteControlMode) -> Result<RemoteControlOutput> {
        let _operation_lock = self.acquire_operation_lock().await?;
        self.set_remote_control_locked(mode).await
    }

    async fn set_remote_control_locked(
        &self,
        mode: RemoteControlMode,
    ) -> Result<RemoteControlOutput> {
        let previous_settings = self.load_settings().await?;
        let mut settings = previous_settings.clone();
        let remote_control_enabled = mode.is_enabled();
        let backend = self.running_backend_instance(&previous_settings).await?;

        if backend.is_none() {
            self.ensure_app_server_absent().await?;
        }

        let changed = settings.remote_control_enabled != remote_control_enabled;
        if changed {
            settings.remote_control_enabled = remote_control_enabled;
            settings.save(&self.settings_file).await?;
        }

        let info = match backend.as_ref() {
            Some(backend) => {
                let pid = backend.running_pid().await?.ok_or_else(|| {
                    anyhow!("managed app server disappeared before readiness probe")
                })?;
                let info = self.wait_until_ready(pid).await?;
                match mode {
                    RemoteControlMode::Enabled => {
                        remote_control_client::enable_remote_control(&self.socket_path).await?;
                    }
                    RemoteControlMode::Disabled => {
                        remote_control_client::disable_remote_control(&self.socket_path).await?;
                    }
                }
                Some(info)
            }
            None => None,
        };

        Ok(self.remote_control_output(
            if changed {
                remote_control_status(mode)
            } else {
                already_remote_control_status(mode)
            },
            info.as_ref().map(|_| BackendKind::Pid),
            info.as_ref().map(|info| info.process_id),
            remote_control_enabled,
            info.map(|info| info.app_server_version),
        ))
    }

    async fn bootstrap_locked(&self, options: BootstrapOptions) -> Result<BootstrapOutput> {
        self.ensure_managed_codex_bin()?;

        let settings = DaemonSettings {
            remote_control_enabled: options.remote_control_enabled,
        };
        if let Ok(info) = client::probe(&self.socket_path).await {
            let Some(backend) = self.running_backend_instance(&settings).await? else {
                return Err(anyhow!(
                    "app server is running but is not managed by codex app-server daemon"
                ));
            };
            self.verify_probe_owner(&backend, info.process_id).await?;
        }
        settings.save(&self.settings_file).await?;

        if let Some(backend) = self.running_backend_instance(&settings).await? {
            backend.stop().await?;
        }

        self.ensure_app_server_absent().await?;
        let backend = backend::pid_backend(self.backend_paths(&settings));
        let pid = backend
            .start()
            .await?
            .ok_or_else(|| anyhow!("managed app server start omitted its pid"))?;
        let info = self.wait_until_ready(pid).await?;
        let managed_codex_version = self.managed_codex_version_best_effort().await;
        Ok(BootstrapOutput {
            status: BootstrapStatus::Bootstrapped,
            backend: BackendKind::Pid,
            auto_update_enabled: false,
            remote_control_enabled: settings.remote_control_enabled,
            managed_codex_path: self.managed_codex_bin.clone(),
            managed_codex_version,
            socket_path: self.socket_path.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            app_server_version: info.app_server_version,
        })
    }

    async fn running_backend_instance(
        &self,
        settings: &DaemonSettings,
    ) -> Result<Option<backend::PidBackend>> {
        let backend = backend::pid_backend(self.backend_paths(settings));
        if backend.is_starting_or_running().await? {
            return Ok(Some(backend));
        }
        Ok(None)
    }

    async fn start_managed_backend(&self, settings: &DaemonSettings) -> Result<Option<u32>> {
        self.start_managed_backend_with_bin(settings, &self.managed_codex_bin)
            .await
    }

    async fn start_managed_backend_with_bin(
        &self,
        settings: &DaemonSettings,
        managed_codex_bin: &Path,
    ) -> Result<Option<u32>> {
        let backend =
            backend::pid_backend(self.backend_paths_with_bin(settings, managed_codex_bin));
        backend.start().await
    }

    async fn is_bootstrapped(&self, settings: &DaemonSettings) -> Result<bool> {
        let _ = settings;
        Ok(self.settings_file.try_exists()?)
    }

    fn ensure_managed_codex_bin(&self) -> Result<()> {
        if self.managed_codex_bin.is_file() {
            return Ok(());
        }

        let managed_codex_path = self.managed_codex_bin.display();
        Err(anyhow!(
            "managed standalone Codex install not found at {managed_codex_path}\n\n\
             This command requires the standalone install managed by the Codex installer, because \
             the daemon starts and updates app-server from that fixed path.\n\n\
             Install it with:\n  curl -fsSL https://chatgpt.com/codex/install.sh | sh\n\n\
             Then rerun the command you just tried."
        ))
    }

    #[cfg(unix)]
    async fn managed_codex_version_best_effort(&self) -> Option<String> {
        managed_codex_version(&self.managed_codex_bin).await.ok()
    }

    #[cfg(not(unix))]
    async fn managed_codex_version_best_effort(&self) -> Option<String> {
        None
    }

    fn backend_paths(&self, settings: &DaemonSettings) -> BackendPaths {
        self.backend_paths_with_bin(settings, &self.managed_codex_bin)
    }

    fn backend_paths_with_bin(
        &self,
        settings: &DaemonSettings,
        managed_codex_bin: &Path,
    ) -> BackendPaths {
        BackendPaths {
            codex_bin: managed_codex_bin.to_path_buf(),
            pid_file: self.pid_file.clone(),
            remote_control_enabled: settings.remote_control_enabled,
        }
    }

    async fn load_settings(&self) -> Result<DaemonSettings> {
        DaemonSettings::load(&self.settings_file).await
    }

    async fn acquire_operation_lock(&self) -> Result<tokio::fs::File> {
        let operation_lock = self.open_operation_lock_file().await?;
        let deadline = tokio::time::Instant::now() + OPERATION_LOCK_TIMEOUT;
        while !try_lock_file(&operation_lock)? {
            if tokio::time::Instant::now() >= deadline {
                return Err(anyhow!(
                    "timed out waiting for daemon operation lock {}",
                    self.operation_lock_file.display()
                ));
            }
            sleep(START_POLL_INTERVAL).await;
        }
        Ok(operation_lock)
    }

    async fn open_operation_lock_file(&self) -> Result<tokio::fs::File> {
        if let Some(parent) = self.operation_lock_file.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "failed to create daemon state directory {}",
                    parent.display()
                )
            })?;
        }
        tokio::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.operation_lock_file)
            .await
            .with_context(|| {
                format!(
                    "failed to open daemon operation lock {}",
                    self.operation_lock_file.display()
                )
            })
    }

    async fn output(
        &self,
        status: LifecycleStatus,
        backend: Option<BackendKind>,
        pid: Option<u32>,
        app_server_version: Option<String>,
    ) -> LifecycleOutput {
        let managed_codex_version = self.managed_codex_version_best_effort().await;
        LifecycleOutput {
            status,
            backend,
            pid,
            managed_codex_path: self.managed_codex_bin.clone(),
            managed_codex_version,
            socket_path: self.socket_path.clone(),
            cli_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            app_server_version,
        }
    }

    fn remote_control_output(
        &self,
        status: RemoteControlStatus,
        backend: Option<BackendKind>,
        pid: Option<u32>,
        remote_control_enabled: bool,
        app_server_version: Option<String>,
    ) -> RemoteControlOutput {
        RemoteControlOutput {
            status,
            backend,
            pid,
            remote_control_enabled,
            socket_path: self.socket_path.clone(),
            cli_version: env!("CARGO_PKG_VERSION").to_string(),
            app_server_version,
        }
    }
}

fn verify_process_identity(expected_pid: u32, observed_pid: u32) -> Result<()> {
    if expected_pid == observed_pid {
        return Ok(());
    }
    bail!("app-server socket owner pid {observed_pid} does not match managed pid {expected_pid}")
}

fn remote_control_status(mode: RemoteControlMode) -> RemoteControlStatus {
    match mode {
        RemoteControlMode::Enabled => RemoteControlStatus::Enabled,
        RemoteControlMode::Disabled => RemoteControlStatus::Disabled,
    }
}

fn already_remote_control_status(mode: RemoteControlMode) -> RemoteControlStatus {
    match mode {
        RemoteControlMode::Enabled => RemoteControlStatus::AlreadyEnabled,
        RemoteControlMode::Disabled => RemoteControlStatus::AlreadyDisabled,
    }
}

#[cfg(unix)]
fn try_lock_file(file: &tokio::fs::File) -> Result<bool> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(true);
    }

    let err = std::io::Error::last_os_error();
    if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
        return Ok(false);
    }
    Err(err).context("failed to lock daemon operation")
}

#[cfg(not(unix))]
fn try_lock_file(_file: &tokio::fs::File) -> Result<bool> {
    Ok(true)
}

#[cfg(all(test, unix))]
mod tests {
    use pretty_assertions::assert_eq;
    use tempfile::TempDir;

    use super::BackendKind;
    use super::BootstrapOutput;
    use super::BootstrapStatus;
    use super::Daemon;
    use super::LifecycleOutput;
    use super::LifecycleStatus;
    use super::RemoteControlStartOutput;
    use super::RemoteControlStatus;
    use super::verify_process_identity;

    #[test]
    fn remote_control_status_uses_camel_case_json() {
        assert_eq!(
            serde_json::to_string(&RemoteControlStatus::AlreadyEnabled).expect("serialize"),
            "\"alreadyEnabled\""
        );
    }

    #[test]
    fn socket_responder_must_match_the_exact_managed_pid() {
        verify_process_identity(42, 42).expect("matching process");
        let error = verify_process_identity(42, 43).expect_err("mismatched process");
        assert_eq!(
            error.to_string(),
            "app-server socket owner pid 43 does not match managed pid 42"
        );
    }

    #[test]
    fn remote_control_start_output_serializes_inner_output_without_tag() {
        let lifecycle_output = LifecycleOutput {
            status: LifecycleStatus::AlreadyRunning,
            backend: Some(BackendKind::Pid),
            pid: None,
            managed_codex_path: "codex".into(),
            managed_codex_version: Some("1.2.3".to_string()),
            socket_path: "codex.sock".into(),
            cli_version: Some("1.2.3".to_string()),
            app_server_version: Some("1.2.4".to_string()),
        };
        let output = RemoteControlStartOutput::Start(lifecycle_output.clone());

        assert_eq!(
            serde_json::to_value(&lifecycle_output).expect("serialize"),
            serde_json::json!({
                "status": "alreadyRunning",
                "backend": "pid",
                "managedCodexPath": "codex",
                "managedCodexVersion": "1.2.3",
                "socketPath": "codex.sock",
                "cliVersion": "1.2.3",
                "appServerVersion": "1.2.4",
            })
        );
        assert_eq!(
            serde_json::to_value(output).expect("serialize"),
            serde_json::to_value(lifecycle_output).expect("serialize")
        );

        let bootstrap_output = BootstrapOutput {
            status: BootstrapStatus::Bootstrapped,
            backend: BackendKind::Pid,
            auto_update_enabled: false,
            remote_control_enabled: true,
            managed_codex_path: "codex".into(),
            managed_codex_version: Some("1.2.3".to_string()),
            socket_path: "codex.sock".into(),
            cli_version: "1.2.3".to_string(),
            app_server_version: "1.2.4".to_string(),
        };
        let output = RemoteControlStartOutput::Bootstrap(bootstrap_output.clone());

        assert_eq!(
            serde_json::to_value(&bootstrap_output).expect("serialize"),
            serde_json::json!({
                "status": "bootstrapped",
                "backend": "pid",
                "autoUpdateEnabled": false,
                "remoteControlEnabled": true,
                "managedCodexPath": "codex",
                "managedCodexVersion": "1.2.3",
                "socketPath": "codex.sock",
                "cliVersion": "1.2.3",
                "appServerVersion": "1.2.4",
            })
        );
        assert_eq!(
            serde_json::to_value(output).expect("serialize"),
            serde_json::to_value(bootstrap_output).expect("serialize")
        );
    }

    #[tokio::test]
    async fn not_ready_context_reports_daemon_app_server_before_stderr() {
        let temp_dir = TempDir::new().expect("temp dir");
        let daemon = Daemon {
            socket_path: temp_dir.path().join("app-server-control.sock"),
            pid_file: temp_dir.path().join("app-server.pid"),
            operation_lock_file: temp_dir.path().join("daemon.lock"),
            settings_file: temp_dir.path().join("settings.json"),
            managed_codex_bin: temp_dir.path().join("missing-codex"),
        };
        let stderr_log = daemon.pid_file.with_extension("stderr.log");
        tokio::fs::write(&stderr_log, "unexpected argument")
            .await
            .expect("write stderr log");

        assert_eq!(
            daemon.app_server_not_ready_context().await,
            format!(
                "app server did not become ready on {}\n\n\
                 Daemon used app-server:\n  path: {}\n  version: unknown\n\n\
                 Managed app-server stderr ({}):\n  unexpected argument",
                daemon.socket_path.display(),
                daemon.managed_codex_bin.display(),
                stderr_log.display()
            )
        );
    }

    #[tokio::test]
    async fn restart_preparation_requires_positive_socket_absence() {
        let temp_dir = TempDir::new().expect("temp dir");
        let daemon = Daemon {
            socket_path: temp_dir.path().join("app-server-control.sock"),
            pid_file: temp_dir.path().join("app-server.pid"),
            operation_lock_file: temp_dir.path().join("daemon.lock"),
            settings_file: temp_dir.path().join("settings.json"),
            managed_codex_bin: temp_dir.path().join("missing-codex"),
        };

        daemon
            .ensure_app_server_absent()
            .await
            .expect("missing socket proves absence");
        tokio::fs::write(&daemon.socket_path, b"stale or unverified owner")
            .await
            .expect("write ambiguous socket path");

        let error = daemon
            .ensure_app_server_absent()
            .await
            .expect_err("an unprobeable existing socket must be ambiguous");
        assert!(
            error
                .to_string()
                .contains("exists but its owner could not be verified"),
            "unexpected error: {error:#}"
        );
    }

    #[tokio::test]
    async fn start_rejects_an_unverified_socket_owner() {
        let temp_dir = TempDir::new().expect("temp dir");
        let daemon = Daemon {
            socket_path: temp_dir.path().join("app-server-control.sock"),
            pid_file: temp_dir.path().join("app-server.pid"),
            operation_lock_file: temp_dir.path().join("daemon.lock"),
            settings_file: temp_dir.path().join("settings.json"),
            managed_codex_bin: temp_dir.path().join("missing-codex"),
        };
        tokio::fs::write(&daemon.socket_path, b"unverified owner")
            .await
            .expect("write ambiguous socket path");

        let error = daemon
            .start()
            .await
            .expect_err("start must not launch against an ambiguous socket");
        assert!(
            error
                .to_string()
                .contains("exists but its owner could not be verified"),
            "unexpected error: {error:#}"
        );
    }
}
