// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocalProcessIdentity {
    pub pid: u32,
    pub started_at_unix_micros: u64,
}

pub fn local_process_identity(pid: u32) -> Option<LocalProcessIdentity> {
    process_parent(pid).map(|(identity, _)| identity)
}

pub fn verified_process_ancestry(
    identity: LocalProcessIdentity,
) -> Option<Vec<LocalProcessIdentity>> {
    let mut processes = Vec::new();
    let mut pid = identity.pid;
    for _ in 0..128 {
        let current = process_parent(pid)?;
        if processes.iter().any(|(previous, _)| *previous == current.0) {
            return None;
        }
        pid = current.1;
        processes.push(current);
        if pid == 0 || pid == 1 {
            break;
        }
    }
    if processes.first().map(|(process, _)| *process) != Some(identity)
        || (pid > 1)
        || processes
            .iter()
            .any(|(process, parent)| process_parent(process.pid) != Some((*process, *parent)))
    {
        return None;
    }
    Some(processes.into_iter().map(|(process, _)| process).collect())
}

#[cfg(target_os = "macos")]
fn process_parent(pid: u32) -> Option<(LocalProcessIdentity, u32)> {
    let pid = i32::try_from(pid).ok().filter(|pid| *pid > 0)?;
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = std::mem::size_of::<libc::proc_bsdinfo>();
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            i32::try_from(size).ok()?,
        )
    };
    if usize::try_from(read).ok()? != size {
        return None;
    }
    let info = unsafe { info.assume_init() };
    if info.pbi_pid != u32::try_from(pid).ok()? || info.pbi_status == 5 {
        return None;
    }
    Some((
        LocalProcessIdentity {
            pid: info.pbi_pid,
            started_at_unix_micros: info
                .pbi_start_tvsec
                .checked_mul(1_000_000)?
                .checked_add(info.pbi_start_tvusec)?,
        },
        info.pbi_ppid,
    ))
}

#[cfg(not(target_os = "macos"))]
fn process_parent(_pid: u32) -> Option<(LocalProcessIdentity, u32)> {
    None
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn process_ancestry_rejects_changed_birth_and_exited_process() -> anyhow::Result<()> {
        let mut child = tokio::process::Command::new("/bin/sleep")
            .arg("30")
            .kill_on_drop(true)
            .spawn()?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow::anyhow!("missing child pid"))?;
        let identity =
            local_process_identity(pid).ok_or_else(|| anyhow::anyhow!("missing child identity"))?;
        let ancestry = verified_process_ancestry(identity)
            .ok_or_else(|| anyhow::anyhow!("missing child ancestry"))?;
        assert_eq!(ancestry.first(), Some(&identity));
        assert!(
            ancestry
                .iter()
                .any(|process| process.pid == std::process::id())
        );
        assert!(
            verified_process_ancestry(LocalProcessIdentity {
                started_at_unix_micros: identity.started_at_unix_micros + 1,
                ..identity
            })
            .is_none()
        );
        child.kill().await?;
        assert!(local_process_identity(pid).is_none());
        assert!(verified_process_ancestry(identity).is_none());
        Ok(())
    }
}
