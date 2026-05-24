//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//
//  Copyright (c) 2026 Prisma Risk
//  Licensed under the Apache License, Version 2.0
//  https://github.com/prisma-risk/tsoracle
//

// #[PerformanceCriticalPath]

use core::pin::Pin;
use futures::{Stream, StreamExt};
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tsoracle_consensus::{ConsensusDriver, ConsensusError, LeaderState};
use tsoracle_core::{Epoch, PHYSICAL_MS_MAX};

use crate::record;

#[derive(Debug, thiserror::Error)]
pub enum FileDriverError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("decode: {0}")]
    Decode(#[from] record::RecordError),
    #[error("physical_ms {0} exceeds 46-bit maximum")]
    PhysicalMsOutOfRange(u64),
    #[error("state directory {path} is already locked by another FileDriver: {source}")]
    AlreadyLocked {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug)]
pub struct FileDriver {
    dir: PathBuf,
    // Published high-water for readers. Writers are externally serialized by
    // `write_lock`, so this is a publish-to-readers cell, not a mutual-exclusion
    // lock. Reads (`load_high_water`) are wait-free; writers do disk I/O
    // without holding any state lock and then publish via a Release store.
    state: Arc<AtomicU64>,
    write_lock: tokio::sync::Mutex<()>,
    // Held to keep the watch channel open; FileDriver never sends after the
    // initial Leader { epoch: 0 } published at construction. Dropping it would
    // close the channel and terminate every `WatchStream::new(leader_rx.clone())`
    // consumer prematurely.
    #[expect(
        dead_code,
        reason = "kept to hold the watch channel open for leader_rx consumers"
    )]
    leader_tx: watch::Sender<LeaderState>,
    leader_rx: watch::Receiver<LeaderState>,
    // Holds the OS-level exclusive lock on `dir/LOCK` for the driver's
    // lifetime. The kernel releases the flock when this file is closed —
    // on graceful Drop, on panic unwind, and on hard process death — so
    // there is no stale-lock cleanup path to maintain.
    _lock: fs::File,
}

impl FileDriver {
    /// Open the state directory. Creates it if missing. Reads and validates the
    /// state file if present. Single-node deployments serve `Leader { epoch: 0 }`
    /// continuously.
    ///
    /// Acquires an exclusive OS-level lock on the `LOCK` sentinel file under
    /// `dir` before reading state, and holds it for the lifetime of the
    /// returned driver. A second concurrent `open_or_init` against the same
    /// directory returns [`FileDriverError::AlreadyLocked`] immediately —
    /// `FileDriver` enforces its one-writer-per-directory precondition rather
    /// than trusting the operator to. The lock is released by the kernel when
    /// the driver is dropped or the process exits (including crash).
    pub fn open_or_init(dir: impl AsRef<Path>) -> Result<Arc<Self>, FileDriverError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        // Lock BEFORE reading state so the in-memory snapshot can't race a
        // concurrent writer in another process. The sentinel is a stable
        // inode — `write_record` replaces `state` via atomic rename, so a
        // lock held on `state` itself would not cover the post-rename file.
        let lock_path = dir.join("LOCK");
        let lock_file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        acquire_exclusive_lock(&lock_file, &lock_path)?;

        let state_path = dir.join("state");
        let current = if state_path.exists() {
            let bytes = fs::read(&state_path)?;
            let high_water = record::decode(&bytes)?;
            if high_water > PHYSICAL_MS_MAX {
                return Err(FileDriverError::PhysicalMsOutOfRange(high_water));
            }
            high_water
        } else {
            0
        };
        let (tx, rx) = watch::channel(LeaderState::Leader { epoch: Epoch::ZERO });
        Ok(Arc::new(FileDriver {
            dir,
            state: Arc::new(AtomicU64::new(current)),
            write_lock: tokio::sync::Mutex::new(()),
            leader_tx: tx,
            leader_rx: rx,
            _lock: lock_file,
        }))
    }

    /// Seed a fresh state directory with a high-water value. Used by the `init`
    /// CLI subcommand for migrations. Fails if state already exists.
    ///
    /// The stored high-water is a physical_ms (the same units the allocator
    /// uses for `committed_high_water`), NOT a packed `Timestamp`. The seed
    /// argument is interpreted as the maximum physical_ms ever observed in the
    /// prior system; on first serve, the failover fence will advance above it.
    pub fn init_seeded(
        dir: impl AsRef<Path>,
        seed_physical_ms: u64,
    ) -> Result<(), FileDriverError> {
        if seed_physical_ms > PHYSICAL_MS_MAX {
            return Err(FileDriverError::PhysicalMsOutOfRange(seed_physical_ms));
        }
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let state_path = dir.join("state");
        if state_path.exists() {
            return Err(FileDriverError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "state file already exists; refusing to overwrite",
            )));
        }
        write_record(dir, seed_physical_ms)?;
        Ok(())
    }
}

/// Try-acquire an exclusive flock on `lock_file`. Classify the contended
/// case (another live `FileDriver` holds it) as
/// [`FileDriverError::AlreadyLocked`]; any other I/O error becomes
/// [`FileDriverError::Io`].
///
/// We don't trust `io::Error::kind()` alone here: on Unix the contended
/// errno is `EWOULDBLOCK` (mapped to `ErrorKind::WouldBlock`), but on
/// Windows `LockFileEx` returns `ERROR_LOCK_VIOLATION`, which stdlib does
/// not necessarily map to `WouldBlock`. `fs2::lock_contended_error()`
/// returns the exact `io::Error` shape the platform uses, so we match on
/// `raw_os_error()` for a portable check.
fn acquire_exclusive_lock(lock_file: &fs::File, lock_path: &Path) -> Result<(), FileDriverError> {
    use fs2::FileExt;
    match lock_file.try_lock_exclusive() {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == fs2::lock_contended_error().raw_os_error() => {
            Err(FileDriverError::AlreadyLocked {
                path: lock_path.to_path_buf(),
                source: err,
            })
        }
        Err(err) => Err(FileDriverError::Io(err)),
    }
}

fn write_record(dir: &Path, high_water: u64) -> Result<(), FileDriverError> {
    tsoracle_failpoint::failpoint!(
        "file_driver::before_write",
        |arg: Option<String>| -> Result<(), FileDriverError> {
            let _ = arg; // currently only one action shape; future tags can match here
            Err(FileDriverError::Io(std::io::Error::other(
                "failpoint: file_driver::before_write",
            )))
        }
    );

    let tmp = dir.join("state.tmp");
    let final_path = dir.join("state");
    let bytes = record::encode(high_water);

    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    tsoracle_failpoint::failpoint!(
        "file_driver::after_tmp_fsync_before_rename",
        |arg: Option<String>| -> Result<(), FileDriverError> {
            let _ = arg;
            Err(FileDriverError::Io(std::io::Error::other(
                "failpoint: file_driver::after_tmp_fsync_before_rename",
            )))
        }
    );

    fs::rename(&tmp, &final_path)?;

    tsoracle_failpoint::failpoint!("file_driver::after_rename_before_dir_fsync");

    // Force the rename's metadata to durable media. The tmpfile `sync_all`
    // above keeps the *data* durable on both platforms; this block adds the
    // *metadata* barrier that makes the new directory entry survive a crash.
    //
    // Unix: open the parent directory and `fsync` its descriptor. This is
    // the canonical POSIX barrier for a rename — it flushes the directory
    // entry that names the new inode.
    //
    // Windows: there is no portable directory-level flush. `FlushFileBuffers`
    // on a directory handle is undefined for most filesystems. NTFS journals
    // `MoveFileEx` as a metadata transaction, but the `$LogFile` record is
    // itself only durable after a checkpoint or an explicit
    // `FlushFileBuffers` on a file on the same volume. Re-opening the
    // renamed file with write access (required by `FlushFileBuffers`) and
    // calling `sync_all` flushes the journal entry covering this rename.
    // This is the pattern SQLite and RocksDB use on Windows.
    #[cfg(unix)]
    {
        let dir_file = fs::File::open(dir)?;
        let fd = dir_file.as_raw_fd();
        // SAFETY: fd is a valid open directory descriptor for the duration of this call.
        let rc = unsafe { libc::fsync(fd) };
        if rc != 0 {
            return Err(FileDriverError::Io(std::io::Error::last_os_error()));
        }
    }
    #[cfg(not(unix))]
    {
        // `write(true)` is required: `FlushFileBuffers` rejects handles
        // without `GENERIC_WRITE`. Default `truncate: false` leaves the
        // file contents (the record we just renamed into place) intact.
        let final_file = fs::OpenOptions::new().write(true).open(&final_path)?;
        final_file.sync_all()?;
    }
    Ok(())
}

#[async_trait::async_trait]
impl ConsensusDriver for FileDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(WatchStream::new(self.leader_rx.clone()).boxed())
    }

    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        // Wait-free read; pairs with the Release store in `persist_high_water`.
        Ok(self.state.load(Ordering::Acquire))
    }

    async fn persist_high_water(
        &self,
        at_least: u64,
        _epoch: Epoch,
    ) -> Result<u64, ConsensusError> {
        if at_least > PHYSICAL_MS_MAX {
            return Err(ConsensusError::PermanentDriver(Box::new(
                FileDriverError::PhysicalMsOutOfRange(at_least),
            )));
        }

        // `write_lock` serializes writers — no two `persist_high_water` calls
        // can race the disk write or the publish step below.
        let _guard = self.write_lock.lock().await;

        let current = self.state.load(Ordering::Acquire);
        if at_least <= current {
            return Ok(current);
        }
        let target = at_least;

        let dir = self.dir.clone();
        tokio::task::spawn_blocking(move || {
            tsoracle_failpoint::failpoint!("file_driver::write_blocked");
            write_record(&dir, target)
        })
        .await
        // spawn_blocking JoinError: the worker thread panicked. That is a
        // bug, not a transient condition — fail permanently.
        .map_err(|e| ConsensusError::PermanentDriver(Box::new(std::io::Error::other(e))))?
        // FileDriverError covers the disk path: I/O failure, CRC/length
        // checks, fsync failure. None of these are safely retried at this
        // layer without operator visibility (a stuck disk does not clear
        // itself). Classify as permanent.
        .map_err(|e| ConsensusError::PermanentDriver(Box::new(e)))?;

        // Publish only after the disk write is durable. Release pairs with
        // the Acquire load in `load_high_water` and the snapshot above.
        self.state.store(target, Ordering::Release);
        Ok(target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn fresh_init_starts_at_zero() {
        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        assert_eq!(driver.load_high_water().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn persist_then_reload() {
        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        let actual = driver.persist_high_water(12345, Epoch::ZERO).await.unwrap();
        assert_eq!(actual, 12345);
        drop(driver);
        let reopened = FileDriver::open_or_init(dir.path()).unwrap();
        assert_eq!(reopened.load_high_water().await.unwrap(), 12345);
    }

    #[tokio::test]
    async fn persist_is_monotonic() {
        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        assert_eq!(
            driver.persist_high_water(100, Epoch::ZERO).await.unwrap(),
            100
        );
        assert_eq!(
            driver.persist_high_water(50, Epoch::ZERO).await.unwrap(),
            100
        );
        assert_eq!(
            driver.persist_high_water(200, Epoch::ZERO).await.unwrap(),
            200
        );
    }

    #[tokio::test]
    async fn init_seeded_rejects_existing_state() {
        let dir = tempdir().unwrap();
        FileDriver::init_seeded(dir.path(), 1_700_000_000_000).unwrap();
        let err = FileDriver::init_seeded(dir.path(), 1_700_000_000_000).unwrap_err();
        match err {
            FileDriverError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::AlreadyExists),
            _ => panic!("expected AlreadyExists"),
        }
    }

    #[tokio::test]
    async fn init_seeded_reloads_as_physical_ms() {
        // The seed argument is a physical_ms; on reload the driver reports the
        // same value (NOT shifted into a packed Timestamp). The allocator's
        // bounds and the file driver's stored value must use identical units.
        let dir = tempdir().unwrap();
        let seed = 1_700_000_000_000u64;
        FileDriver::init_seeded(dir.path(), seed).unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        assert_eq!(driver.load_high_water().await.unwrap(), seed);
        assert!(seed < tsoracle_core::PHYSICAL_MS_MAX);
    }

    #[tokio::test]
    async fn init_seeded_rejects_out_of_range_physical_ms() {
        let dir = tempdir().unwrap();
        let err = FileDriver::init_seeded(dir.path(), PHYSICAL_MS_MAX + 1).unwrap_err();
        assert!(matches!(err, FileDriverError::PhysicalMsOutOfRange(_)));
    }

    #[tokio::test]
    async fn persist_rejects_out_of_range_physical_ms() {
        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        let err = driver
            .persist_high_water(PHYSICAL_MS_MAX + 1, Epoch::ZERO)
            .await
            .unwrap_err();
        assert!(matches!(err, ConsensusError::PermanentDriver(_)));
    }

    #[tokio::test]
    async fn open_or_init_rejects_out_of_range_state() {
        // Hand-write a state file whose encoded high_water exceeds the
        // 46-bit physical_ms cap. open_or_init must refuse to load it
        // rather than silently propagating an invariant violation into
        // the allocator.
        let dir = tempdir().unwrap();
        let state_path = dir.path().join("state");
        let bytes = record::encode(PHYSICAL_MS_MAX + 1);
        fs::write(&state_path, bytes).unwrap();
        let err = FileDriver::open_or_init(dir.path()).unwrap_err();
        assert!(
            matches!(err, FileDriverError::PhysicalMsOutOfRange(v) if v == PHYSICAL_MS_MAX + 1)
        );
    }

    #[tokio::test]
    async fn leadership_events_emits_initial_leader_at_epoch_zero() {
        // FileDriver is single-node by design: every observer sees a single,
        // permanent `Leader { epoch: 0 }` transition on subscription.
        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        let mut stream = driver.leadership_events();
        let first = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("stream emits initial state within the timeout")
            .expect("stream is not closed");
        assert_eq!(first, LeaderState::Leader { epoch: Epoch::ZERO });
    }
}
