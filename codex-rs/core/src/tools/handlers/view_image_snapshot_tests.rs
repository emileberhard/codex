// Modified by Emil Eberhard for RemoteCoder; see REMOTECODER_MODIFICATIONS.md.
use super::*;
use pretty_assertions::assert_eq;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

fn image(byte: u8) -> EncodedImage {
    EncodedImage {
        bytes: Arc::from([byte]),
        mime: "image/png".to_string(),
        source_width: 1,
        source_height: 1,
        width: 1,
        height: 1,
    }
}

#[tokio::test]
async fn evicts_oldest_snapshot_and_keeps_newest_and_refreshed() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let home = AbsolutePathBuf::try_from(home.path())?;
    let mut paths = Vec::new();
    for index in 0..4 {
        let path = save(&home, image(index)).await?;
        fs::File::options()
            .write(true)
            .open(path.as_path())?
            .set_modified(UNIX_EPOCH + Duration::from_secs(u64::from(index)))?;
        paths.push(path);
    }
    let refreshed = save(&home, image(1)).await?;
    assert_eq!(refreshed, paths[1]);
    let directory = home.join("viewed_images");
    let stale_temp = directory.join(format!("{TEMP_PREFIX}stale"));
    let fresh_temp = directory.join(format!("{TEMP_PREFIX}fresh"));
    fs::write(stale_temp.as_path(), [])?;
    fs::write(fresh_temp.as_path(), b"x")?;
    fs::File::options()
        .write(true)
        .open(stale_temp.as_path())?
        .set_modified(UNIX_EPOCH)?;
    prune(
        &directory, &paths[3], /*max_bytes*/ 4, /*max_files*/ 4,
    )?;
    assert_eq!((stale_temp.exists(), fresh_temp.exists()), (false, true));
    assert_eq!(
        paths.iter().map(|path| path.exists()).collect::<Vec<_>>(),
        vec![false, true, true, true]
    );
    Ok(())
}

#[tokio::test]
async fn waits_while_another_process_holds_the_store() -> anyhow::Result<()> {
    let home = tempfile::tempdir()?;
    let home = AbsolutePathBuf::try_from(home.path())?;
    let directory = home.join("viewed_images");
    fs::create_dir_all(directory.as_path())?;
    let other_process = fs::File::create(directory.join(LOCK_NAME).as_path())?;
    other_process.lock()?;
    let pending = tokio::spawn({
        let home = home.clone();
        async move { save(&home, image(7)).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        (
            pending.is_finished(),
            fs::read_dir(directory.as_path())?.count()
        ),
        (false, 1)
    );
    drop(other_process);
    let saved = pending.await??;
    assert_eq!(fs::read(saved.as_path())?, vec![7]);
    Ok(())
}
