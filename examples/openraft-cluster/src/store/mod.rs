//! File-backed RaftLogStorage + RaftStateMachine for openraft 0.10.
//!
//! Layout under `dir`:
//!   dir/
//!     vote              # bincode-encoded openraft Vote (latest)
//!     log/              # one file per log index, named `<index>`
//!     state.json        # last_applied LogId + high_water (atomic rewrite)
//!     snapshot.bin      # most recent snapshot, if any
//!     snapshot.meta.json
//!
//! Pedagogical, not production-grade. Real deployments should use an adapter
//! over a battle-tested KV store (rocksdb, sled).
//!
//! Module layout:
//!   log.rs        — RaftLogStorage + RaftLogReader for FileStore
//!   state.rs      — AppliedState + RaftStateMachine for FileStateMachine
//!                   (entire trait impl block lives here; snapshot methods
//!                   delegate to free functions in snapshot.rs)
//!   snapshot.rs   — StoredSnapshot, snapshot::install/current free fns,
//!                   RaftSnapshotBuilder impl
//!   io.rs         — atomic_write_raw and on-disk format helpers
//!                   (bincode vote, bincode log entries, JSON state,
//!                   raw bytes snapshot + JSON sidecar meta)

mod io;
mod log;
mod snapshot;
mod state;

pub use log::FileStore;
pub use state::{AppliedState, FileStateMachine};

use std::fs;
use std::path::PathBuf;

impl FileStore {
    /// Open (or create) the file-backed store under `dir`.
    ///
    /// Returns `(log_store, state_machine)` as required by `Raft::new`.
    pub async fn open(dir: PathBuf) -> anyhow::Result<(Self, FileStateMachine)> {
        fs::create_dir_all(dir.join("log"))?;

        let vote = io::load_vote(&dir)?;
        let entries = io::load_log_entries(&dir)?;
        let applied = io::load_state(&dir)?;
        let snapshot = io::load_snapshot(&dir, &applied)?;

        let store = FileStore::from_loaded(dir.clone(), vote, entries);
        let sm = FileStateMachine::from_loaded(dir, applied, snapshot);
        Ok((store, sm))
    }
}
