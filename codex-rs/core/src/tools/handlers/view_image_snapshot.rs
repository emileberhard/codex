// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use std::fs;
use std::io;
use std::io::Write;
use std::time::Duration;
use std::time::SystemTime;

use codex_utils_absolute_path::AbsolutePathBuf;
use codex_utils_image::EncodedImage;
use sha2::Digest;
use sha2::Sha256;

const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_SNAPSHOT_FILES: usize = 4096;
const LOCK_NAME: &str = ".lock";
const TEMP_PREFIX: &str = ".viewed-image-";
const TEMP_RETENTION: Duration = Duration::from_secs(60 * 60);

pub(super) async fn save(
    codex_home: &AbsolutePathBuf,
    image: EncodedImage,
) -> io::Result<AbsolutePathBuf> {
    let extension = match image.mime.as_str() {
        "image/png" => "png",
        "image/jpeg" => "jpg",
        "image/gif" => "gif",
        "image/webp" => "webp",
        mime => {
            return Err(io::Error::other(format!(
                "unsupported snapshot MIME type: {mime}"
            )));
        }
    };
    if image.bytes.len() as u64 > MAX_SNAPSHOT_BYTES {
        return Err(io::Error::other(
            "viewed image exceeds snapshot store capacity",
        ));
    }
    let directory = codex_home.join("viewed_images");
    let digest = Sha256::digest(&image.bytes);
    let path = directory.join(format!("{digest:x}.{extension}"));
    let admitted = path.clone();
    tokio::task::spawn_blocking(move || {
        fs::create_dir_all(directory.as_path())?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(directory.join(LOCK_NAME).as_path())?;
        lock.lock()?;
        admit(&directory, &admitted, &image.bytes)?;
        prune(
            &directory,
            &admitted,
            MAX_SNAPSHOT_BYTES,
            MAX_SNAPSHOT_FILES,
        )
    })
    .await??;
    Ok(path)
}

fn admit(directory: &AbsolutePathBuf, path: &AbsolutePathBuf, bytes: &[u8]) -> io::Result<()> {
    match fs::OpenOptions::new().write(true).open(path.as_path()) {
        Ok(file) => file.set_modified(SystemTime::now()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut temporary = tempfile::Builder::new()
                .prefix(TEMP_PREFIX)
                .tempfile_in(directory.as_path())?;
            temporary.write_all(bytes)?;
            temporary
                .persist(path.as_path())
                .map(drop)
                .map_err(|error| error.error)
        }
        Err(error) => Err(error),
    }
}

fn prune(
    directory: &AbsolutePathBuf,
    admitted: &AbsolutePathBuf,
    max_bytes: u64,
    max_files: usize,
) -> io::Result<()> {
    let now = SystemTime::now();
    let mut files = Vec::new();
    let mut bytes = 0;
    let mut count = 0;
    for entry in fs::read_dir(directory.as_path())? {
        let entry = entry?;
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };
        if !metadata.is_file() || entry.file_name() == LOCK_NAME {
            continue;
        }
        let modified = metadata.modified()?;
        if entry.file_name().to_string_lossy().starts_with(TEMP_PREFIX) {
            if now.duration_since(modified).unwrap_or_default() > TEMP_RETENTION {
                remove(&entry.path())?;
            } else {
                bytes += metadata.len();
                count += 1;
            }
            continue;
        }
        bytes += metadata.len();
        count += 1;
        files.push((modified, entry.path(), metadata.len()));
    }
    files.sort_unstable();
    for (_, path, length) in files {
        if bytes <= max_bytes && count <= max_files {
            break;
        }
        if path == admitted.as_path() {
            continue;
        }
        remove(&path)?;
        bytes -= length;
        count -= 1;
    }
    if bytes > max_bytes || count > max_files {
        return Err(io::Error::other(
            "unable to meet viewed image snapshot store limits",
        ));
    }
    Ok(())
}

fn remove(path: &std::path::Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "view_image_snapshot_tests.rs"]
mod tests;
