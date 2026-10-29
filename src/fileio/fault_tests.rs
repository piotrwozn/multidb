use std::{cell::RefCell, collections::VecDeque, error::Error};

use super::*;

enum WriteResult {
    Interrupted,
    Short(usize),
    Failure(io::Error),
    Zero,
}

struct FaultFile {
    file: fs::File,
    writes: VecDeque<WriteResult>,
}

impl Write for FaultFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self.writes.pop_front() {
            Some(WriteResult::Interrupted) => Err(io::ErrorKind::Interrupted.into()),
            Some(WriteResult::Short(limit)) => self.file.write(&bytes[..bytes.len().min(limit)]),
            Some(WriteResult::Failure(error)) => Err(error),
            Some(WriteResult::Zero) => Ok(0),
            None => self.file.write(bytes),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[derive(Default)]
struct FaultFilesystem {
    writes: RefCell<VecDeque<WriteResult>>,
    sync_error: Option<io::ErrorKind>,
    publish_error: Option<io::ErrorKind>,
    directory_error: Option<io::ErrorKind>,
    #[cfg(target_os = "linux")]
    use_full_device: bool,
}

impl AtomicWriteIo for FaultFilesystem {
    type File = FaultFile;

    fn create(&self, path: &Path) -> io::Result<Self::File> {
        let file = Filesystem.create(path)?;
        #[cfg(target_os = "linux")]
        let file = if self.use_full_device {
            drop(file);
            OpenOptions::new().write(true).open("/dev/full")?
        } else {
            file
        };
        Ok(FaultFile {
            file,
            writes: self.writes.borrow_mut().drain(..).collect(),
        })
    }

    fn sync(&self, file: &Self::File) -> io::Result<()> {
        if let Some(error) = self.sync_error {
            return Err(error.into());
        }
        Filesystem.sync(&file.file)
    }

    fn publish(&self, source: &Path, destination: &Path, no_clobber: bool) -> io::Result<()> {
        if let Some(error) = self.publish_error {
            return Err(error.into());
        }
        Filesystem.publish(source, destination, no_clobber)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        if let Some(error) = self.directory_error {
            return Err(error.into());
        }
        Filesystem.sync_directory(path)
    }
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_enospc_from_full_device_does_not_publish_or_leave_temporary_files()
-> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("state");
    fs::write(&path, b"original")?;
    let filesystem = FaultFilesystem {
        use_full_device: true,
        ..FaultFilesystem::default()
    };
    let result = write_file_with(&path, b"replacement", false, &filesystem);
    assert!(matches!(result, Err(error) if error.raw_os_error() == Some(28)));
    assert_eq!(fs::read(&path)?, b"original");
    assert_eq!(fs::read_dir(temp.path())?.count(), 1);
    Ok(())
}

#[test]
fn interrupted_and_short_writes_publish_only_complete_bytes() -> Result<(), Box<dyn Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("state");
    let filesystem = FaultFilesystem {
        writes: RefCell::new(VecDeque::from([
            WriteResult::Interrupted,
            WriteResult::Short(2),
            WriteResult::Short(1),
        ])),
        ..FaultFilesystem::default()
    };
    write_file_with(&path, b"complete-secret-value", false, &filesystem)?;
    assert_eq!(fs::read(&path)?, b"complete-secret-value");
    assert_eq!(fs::read_dir(temp.path())?.count(), 1);
    Ok(())
}

#[test]
fn disk_full_io_and_write_zero_preserve_original_and_remove_partial_temporary_files()
-> Result<(), Box<dyn Error>> {
    for failure in [
        WriteResult::Failure(io::ErrorKind::StorageFull.into()),
        WriteResult::Failure(io::Error::from_raw_os_error(5)),
        WriteResult::Zero,
    ] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("state");
        fs::write(&path, b"original")?;
        let filesystem = FaultFilesystem {
            writes: RefCell::new(VecDeque::from([WriteResult::Short(3), failure])),
            ..FaultFilesystem::default()
        };
        assert!(write_file_with(&path, b"incomplete-secret", false, &filesystem).is_err());
        assert_eq!(fs::read(&path)?, b"original");
        assert_eq!(fs::read_dir(temp.path())?.count(), 1);
    }
    Ok(())
}

#[test]
fn sync_and_publication_failures_preserve_old_data_but_post_publication_sync_is_uncertain()
-> Result<(), Box<dyn Error>> {
    for failure in [
        FaultFilesystem {
            sync_error: Some(io::ErrorKind::Other),
            ..FaultFilesystem::default()
        },
        FaultFilesystem {
            publish_error: Some(io::ErrorKind::StorageFull),
            ..FaultFilesystem::default()
        },
    ] {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("state");
        fs::write(&path, b"old")?;
        assert!(write_file_with(&path, b"complete-new", false, &failure).is_err());
        assert_eq!(fs::read(&path)?, b"old");
        assert_eq!(fs::read_dir(temp.path())?.count(), 1);
    }
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("state");
    fs::write(&path, b"old")?;
    let failure = FaultFilesystem {
        directory_error: Some(io::ErrorKind::Other),
        ..FaultFilesystem::default()
    };
    assert!(write_file_with(&path, b"complete-new", false, &failure).is_err());
    assert_eq!(fs::read(&path)?, b"complete-new");
    assert_eq!(fs::read_dir(temp.path())?.count(), 1);
    Ok(())
}

#[test]
fn new_directory_ancestors_are_synced_and_sync_failure_is_propagated() -> Result<(), Box<dyn Error>>
{
    let temp = tempfile::tempdir()?;
    let first = temp.path().join("first");
    let second = first.join("second");
    let third = second.join("third");
    let mut synced = Vec::new();
    create_dir_all_with(&third, |path| {
        synced.push(path.to_path_buf());
        sync_parent(path)
    })?;
    assert!(synced.starts_with(&[
        fs::canonicalize(&third)?,
        fs::canonicalize(second)?,
        fs::canonicalize(first)?
    ]));
    assert!(synced.len() >= 3);
    let another = third.join("another");
    let failure = create_dir_all_with(&another, |_| Err(io::Error::from_raw_os_error(5)));
    assert!(matches!(failure, Err(error) if error.raw_os_error() == Some(5)));
    assert!(another.is_dir());
    // Retry must sync existing ancestors too: a prior failed attempt may have left
    // them visible without persisting their entries in their own parents.
    assert!(create_dir_all_with(&another, |_| Err(io::ErrorKind::Other.into())).is_err());
    create_dir_all(&another)?;
    Ok(())
}
