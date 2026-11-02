// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LocalProcessIdentity {
    pub pid: u32,
    pub birth: ProcessBirth,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ProcessBirth {
    UnixMicros(u64),
    Linux { boot_id: [u8; 36], start_ticks: u64 },
}

pub fn parse_linux_boot_id(value: &str) -> Option<[u8; 36]> {
    let bytes: [u8; 36] = value.as_bytes().try_into().ok()?;
    for (index, byte) in bytes.iter().enumerate() {
        if [8, 13, 18, 23].contains(&index) {
            if *byte != b'-' {
                return None;
            }
        } else if !byte.is_ascii_digit() && !(b'a'..=b'f').contains(byte) {
            return None;
        }
    }
    Some(bytes)
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
        let Some(current) = process_parent(pid) else {
            break;
        };
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
        || !parents_precede_children(&processes)
        || processes
            .iter()
            .any(|(process, parent)| process_parent(process.pid) != Some((*process, *parent)))
    {
        return None;
    }
    Some(processes.into_iter().map(|(process, _)| process).collect())
}

fn parents_precede_children(processes: &[(LocalProcessIdentity, u32)]) -> bool {
    #[cfg(target_os = "linux")]
    {
        processes
            .windows(2)
            .all(|pair| match (pair[0].0.birth, pair[1].0.birth) {
                (
                    ProcessBirth::Linux {
                        boot_id: child_boot,
                        start_ticks: child_ticks,
                    },
                    ProcessBirth::Linux {
                        boot_id: parent_boot,
                        start_ticks: parent_ticks,
                    },
                ) => child_boot == parent_boot && parent_ticks <= child_ticks,
                _ => false,
            })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = processes;
        true
    }
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
            birth: ProcessBirth::UnixMicros(
                info.pbi_start_tvsec
                    .checked_mul(1_000_000)?
                    .checked_add(info.pbi_start_tvusec)?,
            ),
        },
        info.pbi_ppid,
    ))
}

#[cfg(target_os = "linux")]
fn process_parent(pid: u32) -> Option<(LocalProcessIdentity, u32)> {
    use std::io::Read;

    if pid == 0 {
        return None;
    }
    let read = |path: &str| {
        let mut value = String::new();
        std::fs::File::open(path)
            .ok()?
            .take(1_048_577)
            .read_to_string(&mut value)
            .ok()?;
        (value.len() <= 1_048_576).then_some(value)
    };
    let parse = |text: &str| {
        let (prefix, fields) = text.rsplit_once(") ")?;
        let (actual, _) = prefix.split_once(" (")?;
        if actual.parse::<u32>().ok()? != pid {
            return None;
        }
        let fields = fields.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 20 || matches!(fields[0], "Z" | "X" | "x") {
            return None;
        }
        Some((
            fields[1].parse::<u32>().ok()?,
            fields[19].parse::<u64>().ok()?,
        ))
    };
    let path = format!("/proc/{pid}/stat");
    let before = parse(&read(&path)?)?;
    static BOOT_ID: std::sync::OnceLock<[u8; 36]> = std::sync::OnceLock::new();
    let boot_id = if let Some(boot_id) = BOOT_ID.get() {
        *boot_id
    } else {
        let boot = read("/proc/sys/kernel/random/boot_id")?;
        let boot_id = parse_linux_boot_id(boot.trim())?;
        *BOOT_ID.get_or_init(|| boot_id)
    };
    if parse(&read(&path)?)? != before {
        return None;
    }
    Some((
        LocalProcessIdentity {
            pid,
            birth: ProcessBirth::Linux {
                boot_id,
                start_ticks: before.1,
            },
        },
        before.0,
    ))
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_parent(_pid: u32) -> Option<(LocalProcessIdentity, u32)> {
    None
}

#[cfg(all(test, any(target_os = "macos", target_os = "linux")))]
mod tests {
    use super::*;

    #[cfg(target_os = "linux")]
    #[test]
    fn parent_birth_must_precede_child_on_the_same_boot() {
        let boot_id = *b"01234567-89ab-cdef-0123-456789abcdef";
        let process = |pid, start_ticks, boot_id| LocalProcessIdentity {
            pid,
            birth: ProcessBirth::Linux {
                boot_id,
                start_ticks,
            },
        };
        let child = process(40, 400, boot_id);
        let parent = process(30, 300, boot_id);
        assert!(parents_precede_children(&[(child, 30), (parent, 1)]));
        assert!(!parents_precede_children(&[
            (child, 30),
            (process(30, 401, boot_id), 1)
        ]));
        let mut other_boot = boot_id;
        other_boot[0] = b'1';
        assert!(!parents_precede_children(&[
            (child, 30),
            (process(30, 300, other_boot), 1)
        ]));
        assert!(parse_linux_boot_id("invalid").is_none());
    }

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
                birth: match identity.birth {
                    ProcessBirth::UnixMicros(value) => ProcessBirth::UnixMicros(value + 1),
                    ProcessBirth::Linux {
                        boot_id,
                        start_ticks,
                    } => ProcessBirth::Linux {
                        boot_id,
                        start_ticks: start_ticks + 1
                    },
                },
                ..identity
            })
            .is_none()
        );
        if let ProcessBirth::Linux {
            mut boot_id,
            start_ticks,
        } = identity.birth
        {
            boot_id[0] = if boot_id[0] == b'0' { b'1' } else { b'0' };
            assert!(
                verified_process_ancestry(LocalProcessIdentity {
                    birth: ProcessBirth::Linux {
                        boot_id,
                        start_ticks
                    },
                    ..identity
                })
                .is_none()
            );
        }
        child.kill().await?;
        assert!(local_process_identity(pid).is_none());
        assert!(verified_process_ancestry(identity).is_none());
        Ok(())
    }
}
