use std::{
    sync::{Arc, Barrier},
    time::{Duration, SystemTime},
};

use super::{
    CloudError, CloudLease, CloudLeaseFile, CloudLeaseOptions, ObjectStoreConfig, lock_lease_file,
    read_lease_file, write_lease_file,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[test]
fn broken_or_released_leases_cannot_be_resurrected_by_a_stale_heartbeat() -> TestResult {
    let temp = tempfile::tempdir()?;
    let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
    let mut lease = CloudLease::acquire(uri.clone(), "database")?;
    CloudLease::break_lease(&uri, "database")?;
    assert!(matches!(
        lease.heartbeat(Duration::from_secs(60)),
        Err(CloudError::LeaseOwnerMismatch(_))
    ));
    assert!(CloudLease::validate_fencing_token(&uri, "database", lease.fencing_token()).is_err());
    let mut successor = CloudLease::acquire(uri.clone(), "database")?;
    assert!(lease.release().is_err());
    successor.release()?;
    successor.release()?; // Missing-file release remains idempotent.
    assert!(successor.heartbeat(Duration::from_secs(60)).is_err());
    assert!(
        CloudLease::validate_fencing_token(&uri, "database", successor.fencing_token()).is_err()
    );
    Ok(())
}

#[test]
fn stale_heartbeat_and_release_cannot_modify_a_replacement_owner() -> TestResult {
    let temp = tempfile::tempdir()?;
    let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
    let mut old = CloudLease::acquire(uri.clone(), "database")?;
    let path = temp.path().join("leases/database.lease");
    {
        let _guard = lock_lease_file(&path)?;
        let mut record = read_lease_file(&path)?.ok_or("missing lease")?;
        record.expires_at = SystemTime::UNIX_EPOCH;
        write_lease_file(&path, &record)?;
    }
    let successor = CloudLease::acquire(uri.clone(), "database")?;
    let expected = std::fs::read(&path)?;
    assert!(old.heartbeat(Duration::from_secs(60)).is_err());
    assert!(old.release().is_err());
    assert_eq!(std::fs::read(&path)?, expected);
    CloudLease::validate_fencing_token(&uri, "database", successor.fencing_token())?;
    Ok(())
}

#[test]
fn concurrent_expired_lease_takeovers_have_exactly_one_winner() -> TestResult {
    let temp = tempfile::tempdir()?;
    let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
    let old = CloudLease::acquire(uri.clone(), "database")?;
    let path = temp.path().join("leases/database.lease");
    {
        let _guard = lock_lease_file(&path)?;
        write_lease_file(
            &path,
            &CloudLeaseFile {
                lease_id: old.lease_id().to_owned(),
                owner: old.owner.clone(),
                expires_at: SystemTime::UNIX_EPOCH,
            },
        )?;
    }
    let start = Arc::new(Barrier::new(9));
    let handles = (0..8)
        .map(|_| {
            let start = Arc::clone(&start);
            let uri = uri.clone();
            std::thread::spawn(move || {
                start.wait();
                CloudLease::acquire(uri, "database")
            })
        })
        .collect::<Vec<_>>();
    start.wait();
    let mut winners = Vec::new();
    for handle in handles {
        match handle.join().map_err(|_| "lease thread panicked")? {
            Ok(lease) => winners.push(lease),
            Err(CloudError::LeaseExists(_)) => {}
            Err(error) => return Err(error.into()),
        }
    }
    assert_eq!(winners.len(), 1);
    CloudLease::validate_fencing_token(&uri, "database", winners[0].fencing_token())?;
    assert!(old.release().is_err());
    Ok(())
}

#[test]
fn heartbeat_waits_for_the_stable_lock_then_revalidates_owner() -> TestResult {
    let temp = tempfile::tempdir()?;
    let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
    let mut old = CloudLease::acquire(uri.clone(), "database")?;
    let path = temp.path().join("leases/database.lease");
    let guard = lock_lease_file(&path)?;
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let _ = started_tx.send(());
        let result = old.heartbeat(Duration::from_secs(60));
        let _ = done_tx.send(result);
    });
    started_rx.recv()?;
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_millis(20)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    let replacement = CloudLeaseFile {
        lease_id: "successor".to_owned(),
        owner: "next".to_owned(),
        expires_at: SystemTime::now() + Duration::from_secs(60),
    };
    write_lease_file(&path, &replacement)?;
    drop(guard);
    assert!(matches!(
        done_rx.recv_timeout(Duration::from_secs(2))?,
        Err(CloudError::LeaseOwnerMismatch(_))
    ));
    worker.join().map_err(|_| "lease thread panicked")?;
    CloudLease::validate_fencing_token(&uri, "database", "successor")?;
    Ok(())
}

#[test]
fn overflowing_lease_ttl_is_rejected_without_claiming_ownership() -> TestResult {
    let temp = tempfile::tempdir()?;
    let uri = ObjectStoreConfig::local_dir(temp.path()).uri;
    assert!(
        CloudLease::acquire_with_options(
            uri.clone(),
            "database",
            CloudLeaseOptions {
                ttl: Duration::MAX,
                owner: None
            }
        )
        .is_err()
    );
    assert!(CloudLease::acquire(uri, "database").is_ok());
    Ok(())
}
