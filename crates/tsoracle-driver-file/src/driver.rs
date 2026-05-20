use core::pin::Pin;
use futures::{Stream, StreamExt};
use std::fs;
use std::io::Write;
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
    #[allow(dead_code)]
    leader_tx: watch::Sender<LeaderState>,
    leader_rx: watch::Receiver<LeaderState>,
}

impl FileDriver {
    /// Open the state directory. Creates it if missing. Reads and validates the
    /// state file if present. Single-node deployments serve `Leader { epoch: 0 }`
    /// continuously.
    pub fn open_or_init(dir: impl AsRef<Path>) -> Result<Arc<Self>, FileDriverError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
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

fn write_record(dir: &Path, high_water: u64) -> Result<(), FileDriverError> {
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

    fs::rename(&tmp, &final_path)?;

    // Fsync the directory so the rename is durable.
    let dir_file = fs::File::open(dir)?;
    let fd = dir_file.as_raw_fd();
    // SAFETY: fd is a valid open directory descriptor for the duration of this call.
    let rc = unsafe { libc::fsync(fd) };
    if rc != 0 {
        return Err(FileDriverError::Io(std::io::Error::last_os_error()));
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
            #[cfg(test)]
            {
                let hook = SLOW_WRITE_HOOK.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook();
                }
            }
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

// Test-only hook: lets a unit test pause the writer thread inside
// `spawn_blocking`, just before `write_record`, so we can observe that
// `load_high_water` is not blocked by an in-flight persist. Always `None`
// outside of `cfg(test)` and incurs zero cost in release builds.
#[cfg(test)]
pub(crate) static SLOW_WRITE_HOOK: std::sync::Mutex<Option<Arc<dyn Fn() + Send + Sync>>> =
    std::sync::Mutex::new(None);

// Serializes unit tests that exercise the writer path. The hook above is a
// process-global slot, so tests that install it must not race other tests
// that call `persist_high_water` (those would see the hook and hang).
#[cfg(test)]
pub(crate) static PERSIST_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        let _serial = PERSIST_TEST_SERIAL.lock().await;
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
        let _serial = PERSIST_TEST_SERIAL.lock().await;
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

    /// Regression guard for the bug the AtomicU64 refactor fixed: with the
    /// old `Mutex<u64>` held across `write_record`, a `load_high_water` call
    /// during an in-flight persist would park on the state lock for the full
    /// duration of fsync. We install a hook that holds the writer inside
    /// `spawn_blocking` just before `write_record`, then check that a
    /// concurrent reader completes.
    ///
    /// Failure mode if the regression returns: `load_high_water` would block
    /// on the same sync mutex held by the writer. The `tokio::time::timeout`
    /// below catches *async* regressions cleanly, but a sync-mutex regression
    /// (exactly the original bug) hangs the task off-runtime — the timer
    /// driver never gets to poll the timeout future. In that case this test
    /// hangs until CI's per-test or per-binary timeout fires, which is the
    /// signal we rely on for detection. Run under `cargo nextest` or shell
    /// `timeout` in CI to bound it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn load_is_not_blocked_by_in_flight_persist() {
        use std::sync::mpsc::sync_channel;
        use std::time::Duration;

        let _serial = PERSIST_TEST_SERIAL.lock().await;

        let dir = tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();

        // The hook signals via `entered_tx` once the writer reaches it, then
        // blocks on `release_rx` until the test lets it proceed. Channels of
        // capacity 1 are enough — the hook fires exactly once per call.
        let (entered_tx, entered_rx) = sync_channel::<()>(1);
        let (release_tx, release_rx) = sync_channel::<()>(1);
        let release_rx = std::sync::Mutex::new(release_rx);

        // RAII guard so the hook clears even if an assertion below panics —
        // otherwise a leaked hook would hang every later writer test.
        struct HookGuard;
        impl Drop for HookGuard {
            fn drop(&mut self) {
                *SLOW_WRITE_HOOK.lock().unwrap() = None;
            }
        }
        *SLOW_WRITE_HOOK.lock().unwrap() = Some(Arc::new(move || {
            entered_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        let _hook_guard = HookGuard;

        let writer = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.persist_high_water(100, Epoch::ZERO).await })
        };

        // Deterministic wait: block until the writer is inside the hook.
        // Running the sync recv inside `spawn_blocking` keeps it off a
        // runtime worker thread.
        tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
            .await
            .unwrap();

        // Writer is parked at the "about to fsync" point. A reader must not
        // wait on it. The 500ms timeout below only fires for async-blocking
        // regressions; the sync-mutex regression this guards against shows
        // up as a hang (see the doc comment).
        let read = tokio::time::timeout(Duration::from_millis(500), driver.load_high_water())
            .await
            .expect("load_high_water was blocked by an in-flight persist");
        assert_eq!(
            read.unwrap(),
            0,
            "writer has not published yet; reader should see the prior value"
        );

        // Release the writer and verify it completes and publishes 100.
        release_tx.send(()).unwrap();
        let persisted = writer.await.unwrap().unwrap();
        assert_eq!(persisted, 100);
        assert_eq!(driver.load_high_water().await.unwrap(), 100);
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
}
