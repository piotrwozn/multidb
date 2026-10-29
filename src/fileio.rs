use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use uuid::Uuid;

/// Writes bytes through a same-directory temporary file and atomically replaces the target path.
pub(crate) fn atomic_write(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    write_file(path.as_ref(), bytes, false)
}

/// Publishes a complete private file only if the destination is still absent.
pub(crate) fn atomic_create(path: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
    write_file(path.as_ref(), bytes, true)
}

fn write_file(path: &Path, bytes: &[u8], no_clobber: bool) -> io::Result<()> {
    write_file_with(path, bytes, no_clobber, &Filesystem)
}

/// Keeps the fault boundary at actual writes, file sync and publication. Tests replace only
/// the requested syscall result and still exercise real temporary files and cleanup.
trait AtomicWriteIo {
    type File: Write;
    fn create(&self, path: &Path) -> io::Result<Self::File>;
    fn sync(&self, file: &Self::File) -> io::Result<()>;
    fn publish(&self, source: &Path, destination: &Path, no_clobber: bool) -> io::Result<()>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
}

struct Filesystem;

impl AtomicWriteIo for Filesystem {
    type File = fs::File;

    fn create(&self, path: &Path) -> io::Result<Self::File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(path)
    }

    fn sync(&self, file: &Self::File) -> io::Result<()> {
        file.sync_all()
    }

    fn publish(&self, source: &Path, destination: &Path, no_clobber: bool) -> io::Result<()> {
        if no_clobber {
            // Same-filesystem hard_link never replaces a concurrent file or symlink.
            fs::hard_link(source, destination)
        } else {
            fs::rename(source, destination)
        }
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_parent(path)
    }
}

fn write_file_with(
    path: &Path,
    bytes: &[u8],
    no_clobber: bool,
    filesystem: &impl AtomicWriteIo,
) -> io::Result<()> {
    let parent = parent_directory(path);
    create_dir_all(parent)?;

    let file_name = path
        .file_name()
        .unwrap_or_else(|| OsStr::new("file"))
        .to_string_lossy();
    let tmp_path = parent.join(format!(".{file_name}.{}.tmp", Uuid::now_v7()));
    let mut tmp = filesystem.create(&tmp_path)?;
    let _cleanup = RemoveFileOnDrop(tmp_path.clone());
    tmp.write_all(bytes)?;
    filesystem.sync(&tmp)?;
    drop(tmp);

    filesystem.publish(&tmp_path, path, no_clobber)?;
    filesystem.sync_directory(path)?;
    Ok(())
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Creates directories and persists their ancestor entries, deepest first. Existing ancestors
/// are synced too: they may remain from an earlier attempt that failed before directory sync.
pub(crate) fn create_dir_all(path: &Path) -> io::Result<()> {
    create_dir_all_with(path, sync_parent)
}

fn create_dir_all_with(
    path: &Path,
    mut sync: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    fs::create_dir_all(path)?;
    // A parent may be a symlink. Sync the actual directory tree, including on retry,
    // rather than only the ancestors of its alternate spelling.
    let absolute = fs::canonicalize(path)?;
    for directory in absolute
        .ancestors()
        .filter(|directory| directory.parent().is_some())
    {
        sync(directory)?;
    }
    Ok(())
}

pub(crate) fn sync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    fs::File::open(parent_directory(path))?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

struct RemoveFileOnDrop(PathBuf);

impl Drop for RemoveFileOnDrop {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// An exclusive temporary directory, inaccessible to other users on Unix.
pub(crate) struct PrivateDirectory(PathBuf);

impl PrivateDirectory {
    pub(crate) fn create_in(parent: &Path, prefix: &str) -> io::Result<Self> {
        let path = parent.join(format!(".{prefix}-{}", Uuid::now_v7()));
        let mut builder = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(&path)?;
        Ok(Self(path))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for PrivateDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::{PrivateDirectory, atomic_create, atomic_write};

    #[test]
    fn atomic_write_replaces_with_complete_file() -> Result<(), Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("state.json");

        atomic_write(&path, b"old-complete")?;
        atomic_write(&path, b"new-complete")?;

        assert_eq!(std::fs::read(path)?, b"new-complete");
        Ok(())
    }

    #[test]
    fn atomic_create_never_clobbers_and_failure_cleans_temporary_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("state");
        atomic_create(&path, b"original")?;
        assert!(
            matches!(atomic_create(&path, b"replacement"), Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists)
        );
        assert_eq!(std::fs::read(&path)?, b"original");
        let directory = temp.path().join("directory");
        std::fs::create_dir(&directory)?;
        assert!(atomic_write(&directory, b"must not replace directory").is_err());
        assert_eq!(std::fs::read_dir(temp.path())?.count(), 2);
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn private_writes_and_directories_do_not_expose_secrets()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("private");
        atomic_write(&path, b"secret")?;
        assert_eq!(
            std::fs::metadata(&path)?.permissions().mode() & 0o777,
            0o600
        );
        let directory = PrivateDirectory::create_in(temp.path(), "drill")?;
        let private = directory.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&private)?.permissions().mode() & 0o777,
            0o700
        );
        std::fs::write(private.join("decrypted"), b"secret")?;
        drop(directory);
        assert!(!private.exists());
        Ok(())
    }
}

#[cfg(test)]
mod fault_tests;
