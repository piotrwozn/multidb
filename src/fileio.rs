use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::Path,
};

use uuid::Uuid;

/// Writes bytes through a same-directory temporary file and atomically replaces the target path.
pub(crate) fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    let path = path.as_ref();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .unwrap_or_else(|| OsStr::new("file"))
        .to_string_lossy();
    let tmp_path = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));
    let mut tmp = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    tmp.write_all(bytes)?;
    tmp.sync_all()?;
    drop(tmp);

    let result = replace_file(&tmp_path, path);
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result?;
    sync_parent(path);
    Ok(())
}

fn replace_file(tmp_path: &Path, target_path: &Path) -> io::Result<()> {
    match fs::rename(tmp_path, target_path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            fs::remove_file(target_path)?;
            fs::rename(tmp_path, target_path)
        }
        Err(error) => Err(error),
    }
}

fn sync_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::File::open(parent).and_then(|dir| dir.sync_all());
    }
}

#[cfg(test)]
mod tests {
    use super::atomic_write;

    #[test]
    fn atomic_write_replaces_with_complete_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.json");

        atomic_write(&path, b"old-complete")?;
        atomic_write(&path, b"new-complete")?;

        assert_eq!(std::fs::read(path)?, b"new-complete");
        Ok(())
    }
}
