//! On-disk format helpers and the atomic write primitive.
//!
//! All other files in this module reach disk only through these functions —
//! file layout and encoding choices live in one place.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;

use openraft::storage::SnapshotMeta;
use openraft::type_config::alias::{EntryOf, SnapshotMetaOf, VoteOf};

use crate::types::TypeConfig;

use super::snapshot::StoredSnapshot;
use super::state::AppliedState;

type Entry = EntryOf<TypeConfig>;
type Vote = VoteOf<TypeConfig>;
type SnapMeta = SnapshotMetaOf<TypeConfig>;

/// Write `bytes` to `path` atomically via a tmp file + rename.
pub(super) fn atomic_write_raw(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

pub(super) fn load_vote(dir: &Path) -> io::Result<Option<Vote>> {
    match fs::read(dir.join("vote")) {
        Ok(b) => postcard::from_bytes::<Vote>(&b)
            .map(Some)
            .map_err(|e| io::Error::other(e.to_string())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

pub(super) fn save_vote(dir: &Path, vote: &Vote) -> io::Result<()> {
    let bytes = postcard::to_stdvec(vote).map_err(|e| io::Error::other(e.to_string()))?;
    atomic_write_raw(&dir.join("vote"), &bytes)
}

pub(super) fn load_log_entries(dir: &Path) -> io::Result<BTreeMap<u64, Entry>> {
    let mut out: BTreeMap<u64, Entry> = BTreeMap::new();
    for item in fs::read_dir(dir.join("log"))? {
        let item = item?;
        let name = item.file_name().into_string().unwrap_or_default();
        if let Ok(idx) = name.parse::<u64>() {
            let bytes = fs::read(item.path())?;
            let entry: Entry =
                postcard::from_bytes(&bytes).map_err(|e| io::Error::other(e.to_string()))?;
            out.insert(idx, entry);
        }
    }
    Ok(out)
}

pub(super) fn save_log_entry(dir: &Path, idx: u64, entry: &Entry) -> io::Result<()> {
    let bytes = postcard::to_stdvec(entry).map_err(|e| io::Error::other(e.to_string()))?;
    atomic_write_raw(&dir.join("log").join(idx.to_string()), &bytes)
}

pub(super) fn remove_log_entry(dir: &Path, idx: u64) -> io::Result<()> {
    match fs::remove_file(dir.join("log").join(idx.to_string())) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

pub(super) fn load_state(dir: &Path) -> io::Result<AppliedState> {
    match fs::read(dir.join("state.json")) {
        Ok(b) => serde_json::from_slice(&b).map_err(|e| io::Error::other(e.to_string())),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(AppliedState::default()),
        Err(e) => Err(e),
    }
}

pub(super) fn save_state(dir: &Path, state: &AppliedState) -> io::Result<()> {
    let bytes = serde_json::to_vec(state).map_err(|e| io::Error::other(e.to_string()))?;
    atomic_write_raw(&dir.join("state.json"), &bytes)
}

pub(super) fn load_snapshot(
    dir: &Path,
    applied: &AppliedState,
) -> io::Result<Option<StoredSnapshot>> {
    let data = match fs::read(dir.join("snapshot.bin")) {
        Ok(d) => d,
        Err(_) => return Ok(None),
    };
    let meta = match fs::read(dir.join("snapshot.meta.json")) {
        Ok(mb) => serde_json::from_slice(&mb)
            .ok()
            .unwrap_or_else(|| SnapshotMeta {
                last_log_id: applied.last_applied,
                last_membership: applied.last_membership.clone(),
                snapshot_id: "recovered".into(),
            }),
        Err(_) => return Ok(None),
    };
    Ok(Some(StoredSnapshot { meta, data }))
}

pub(super) fn save_snapshot(dir: &Path, meta: &SnapMeta, bytes: &[u8]) -> io::Result<()> {
    atomic_write_raw(&dir.join("snapshot.bin"), bytes)?;
    let meta_bytes = serde_json::to_vec(meta).map_err(|e| io::Error::other(e.to_string()))?;
    atomic_write_raw(&dir.join("snapshot.meta.json"), &meta_bytes)?;
    Ok(())
}
