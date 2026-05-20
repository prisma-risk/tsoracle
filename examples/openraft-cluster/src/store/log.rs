//! File-backed log store. Implements RaftLogStorage + RaftLogReader.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use openraft::RaftLogReader;
use openraft::storage::{IOFlushed, LogState, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf, VoteOf};
use tokio::sync::RwLock;

use crate::types::TypeConfig;

use super::io as disk;

type Entry = EntryOf<TypeConfig>;
type LogId = LogIdOf<TypeConfig>;
type Vote = VoteOf<TypeConfig>;

struct FileStoreInner {
    vote: Option<Vote>,
    log: BTreeMap<u64, Entry>,
    last_purged: Option<LogId>,
}

/// File-backed log store. Clone gives a second handle sharing the same state.
pub struct FileStore {
    dir: PathBuf,
    inner: Arc<RwLock<FileStoreInner>>,
}

impl FileStore {
    pub(super) fn from_loaded(dir: PathBuf, vote: Option<Vote>, log: BTreeMap<u64, Entry>) -> Self {
        FileStore {
            dir,
            inner: Arc::new(RwLock::new(FileStoreInner {
                vote,
                log,
                last_purged: None,
            })),
        }
    }
}

impl Clone for FileStore {
    fn clone(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl RaftLogReader<TypeConfig> for FileStore {
    async fn try_get_log_entries<RB>(&mut self, range: RB) -> Result<Vec<Entry>, io::Error>
    where
        RB: RangeBounds<u64> + Clone + Debug + Send,
    {
        let inner = self.inner.read().await;
        Ok(inner.log.range(range).map(|(_, v)| v.clone()).collect())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote>, io::Error> {
        Ok(self.inner.read().await.vote)
    }
}

impl RaftLogStorage<TypeConfig> for FileStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, io::Error> {
        let inner = self.inner.read().await;
        let last = inner.log.values().next_back().map(|e| e.log_id);
        let last_purged = inner.last_purged;
        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        inner.vote = Some(*vote);
        disk::save_vote(&self.dir, vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<TypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = Entry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.write().await;
        for entry in entries {
            let idx = entry.log_id.index;
            disk::save_log_entry(&self.dir, idx, &entry)?;
            inner.log.insert(idx, entry);
        }
        callback.io_completed(Ok(()));
        Ok(())
    }

    async fn truncate_after(&mut self, last_log_id: Option<LogId>) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let start = match &last_log_id {
            Some(id) => id.index + 1,
            None => 0,
        };
        let keys: Vec<u64> = inner.log.range(start..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
            disk::remove_log_entry(&self.dir, k)?;
        }
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId) -> Result<(), io::Error> {
        let mut inner = self.inner.write().await;
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
            disk::remove_log_entry(&self.dir, k)?;
        }
        inner.last_purged = Some(log_id);
        Ok(())
    }
}
