// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
#[cfg(unix)]
use std::io::Error;
#[cfg(unix)]
use std::io::ErrorKind;
use std::io::Result;

#[cfg(unix)]
const REQUIRED_NOFILE_SOFT_LIMIT: libc::rlim_t = 8_192;

/// Ensures that app-server and its descendants can open the files needed by a session.
///
/// The process inherits its hard limit. A higher soft limit is preserved, while a lower
/// soft limit is raised to the required floor when the inherited hard limit permits it.
#[cfg(unix)]
pub fn ensure_nofile_floor() -> Result<()> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: getrlimit initializes the caller-provided rlimit on success.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return Err(Error::other(format!(
            "failed to read RLIMIT_NOFILE: {}",
            Error::last_os_error()
        )));
    }
    // SAFETY: the successful getrlimit call initialized the value.
    let mut limit = unsafe { limit.assume_init() };
    if limit.rlim_cur >= REQUIRED_NOFILE_SOFT_LIMIT {
        return Ok(());
    }
    if limit.rlim_max != libc::RLIM_INFINITY && limit.rlim_max < REQUIRED_NOFILE_SOFT_LIMIT {
        return Err(Error::new(
            ErrorKind::PermissionDenied,
            format!(
                "RLIMIT_NOFILE hard limit {} is below required soft floor {}",
                limit.rlim_max, REQUIRED_NOFILE_SOFT_LIMIT
            ),
        ));
    }

    limit.rlim_cur = REQUIRED_NOFILE_SOFT_LIMIT;
    // SAFETY: `limit` is initialized from getrlimit and retains the inherited hard limit.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(Error::other(format!(
            "failed to raise RLIMIT_NOFILE soft limit to {} (hard limit {}): {}",
            REQUIRED_NOFILE_SOFT_LIMIT,
            limit.rlim_max,
            Error::last_os_error()
        )));
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn ensure_nofile_floor() -> Result<()> {
    Ok(())
}

#[cfg(all(test, unix))]
#[path = "resources_tests.rs"]
mod tests;
