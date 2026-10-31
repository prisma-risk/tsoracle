//
//  ░▀█▀░█▀▀░█▀█░█▀▄░█▀█░█▀▀░█░░░█▀▀
//  ░░█░░▀▀█░█░█░█▀▄░█▀█░█░░░█░░░█▀▀
//  ░░▀░░▀▀▀░▀▀▀░▀░▀░▀░▀░▀▀▀░▀▀▀░▀▀▀
//
//  tsoracle — Distributed Timestamp Oracle
//  https://www.tsoracle.rs
//
//  Copyright (c) 2026 Prisma Risk
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::driver::{
    DEFAULT_DENSE_CARDINALITY_CAP, DENSE_FILE, LEASES_FILE, LOCK_FILE, STATE_FILE,
    acquire_exclusive_lock,
};
use crate::{FileDriverError, dense_record, lease_record, record};

const FILES: [&str; 4] = [LOCK_FILE, DENSE_FILE, LEASES_FILE, STATE_FILE];
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error(transparent)]
    Driver(#[from] FileDriverError),
    #[error("I/O at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("state decode at {path}: {source}")]
    StateDecode {
        path: PathBuf,
        #[source]
        source: record::RecordError,
    },
    #[error("dense decode at {path}: {source}")]
    DenseDecode {
        path: PathBuf,
        #[source]
        source: dense_record::DenseRecordError,
    },
    #[error("lease decode at {path}: {source}")]
    LeaseDecode {
        path: PathBuf,
        #[source]
        source: lease_record::LeaseRecordError,
    },
    #[error("stored state at {path}: {source}")]
    Validation {
        path: PathBuf,
        #[source]
        source: crate::StoredStateError,
    },
    #[error("invalid stopped file state: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct Producer {
    pub name: &'static str,
    pub version: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Units {
    pub reservation_and_lease_upper_bound: &'static str,
    pub lease_expiry: &'static str,
    pub lease_ttl: &'static str,
    pub dense_high_water: &'static str,
    pub holder: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileInventoryEntry {
    pub path: String,
    pub present: bool,
    pub size_bytes: Option<u64>,
    pub sha256: Option<String>,
    pub format: &'static str,
    pub format_version: Option<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeaseExport {
    pub lease_id: u64,
    pub holder_hex: String,
    pub holder_epoch: u64,
    pub ttl_ms: u64,
    pub ts_upper_bound_physical_ms: u64,
    pub expires_at_unix_ms: u64,
    pub superseded: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoppedFileReceipt {
    pub schema: &'static str,
    pub schema_version: u32,
    pub producer: Producer,
    pub driver: &'static str,
    pub epoch: u64,
    pub units: Units,
    pub files: Vec<FileInventoryEntry>,
    pub committed_reservation_high_water_physical_ms: u64,
    pub leases: Vec<LeaseExport>,
    pub maximum_lease_upper_bound_physical_ms: Option<u64>,
    pub maximum_lease_expires_at_unix_ms: Option<u64>,
    pub dense_next_unallocated: BTreeMap<String, u64>,
    pub dense_cardinality_cap: u64,
}

/// Exclusion must outlive both the receipt and the copy it certifies.
#[derive(Debug)]
pub struct StoppedFileState {
    dir: PathBuf,
    lock: File,
}

impl StoppedFileState {
    /// The caller must stop the owner and disable restarts before capture.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, ExportError> {
        let dir = dir.as_ref();
        if !fs::symlink_metadata(dir)
            .map_err(|source| io_error(dir, source))?
            .file_type()
            .is_dir()
        {
            return invalid("state directory must be an existing directory without a symlink");
        }
        let lock_path = dir.join(LOCK_FILE);
        let lock = open_regular(&lock_path)?;
        acquire_exclusive_lock(&lock, &lock_path).map_err(|error| match error {
            FileDriverError::Io(source) => io_error(&lock_path, source),
            other => ExportError::Driver(other),
        })?;
        Ok(Self {
            dir: dir.to_path_buf(),
            lock,
        })
    }

    /// Retain this guard through complete-directory capture and hash verification.
    pub fn receipt(&self) -> Result<StoppedFileReceipt, ExportError> {
        self.verify_lock_identity()?;
        for entry in fs::read_dir(&self.dir).map_err(|source| io_error(&self.dir, source))? {
            let entry = entry.map_err(|source| io_error(&self.dir, source))?;
            if !FILES
                .iter()
                .any(|name| entry.file_name() == std::ffi::OsStr::new(name))
            {
                return invalid("unexpected file in state directory, including temporary files");
            }
        }

        let lock_path = self.dir.join(LOCK_FILE);
        if self
            .lock
            .metadata()
            .map_err(|source| io_error(&lock_path, source))?
            .len()
            != 0
        {
            return invalid("LOCK sentinel must be empty");
        }
        let mut lock_bytes = Vec::new();
        (&self.lock)
            .take(1)
            .read_to_end(&mut lock_bytes)
            .map_err(|source| io_error(&lock_path, source))?;
        if !lock_bytes.is_empty() {
            return invalid("LOCK sentinel must be empty");
        }
        let state = read_bounded(&self.dir.join(STATE_FILE))?;
        let dense_bytes = read_optional(&self.dir.join(DENSE_FILE))?;
        let lease_bytes = read_optional(&self.dir.join(LEASES_FILE))?;
        let high_water = record::decode(&state).map_err(|source| ExportError::StateDecode {
            path: self.dir.join(STATE_FILE),
            source,
        })?;
        let (dense, cap) = match &dense_bytes {
            Some(bytes) => {
                dense_record::decode(bytes).map_err(|source| ExportError::DenseDecode {
                    path: self.dir.join(DENSE_FILE),
                    source,
                })?
            }
            None => (BTreeMap::new(), DEFAULT_DENSE_CARDINALITY_CAP),
        };
        let records = match &lease_bytes {
            Some(bytes) => {
                lease_record::decode(bytes).map_err(|source| ExportError::LeaseDecode {
                    path: self.dir.join(LEASES_FILE),
                    source,
                })?
            }
            None => Vec::new(),
        };
        crate::validation::validate(high_water, &dense, cap, &records).map_err(|source| {
            ExportError::Validation {
                path: self.dir.clone(),
                source,
            }
        })?;
        let files = vec![
            inventory(LOCK_FILE, Some(&lock_bytes), "native-lock-sentinel", None),
            inventory(
                DENSE_FILE,
                dense_bytes.as_deref(),
                magic(dense_record::MAGIC)?,
                Some(dense_record::VERSION),
            ),
            inventory(
                LEASES_FILE,
                lease_bytes.as_deref(),
                magic(lease_record::MAGIC)?,
                Some(lease_record::VERSION),
            ),
            inventory(
                STATE_FILE,
                Some(&state),
                magic(record::MAGIC)?,
                Some(record::VERSION),
            ),
        ];
        let leases: Vec<_> = records
            .into_iter()
            .map(|record| LeaseExport {
                lease_id: record.lease_id,
                holder_hex: hex(&record.holder),
                holder_epoch: record.holder_epoch,
                ttl_ms: record.ttl_ms,
                ts_upper_bound_physical_ms: record.ts_upper_bound,
                expires_at_unix_ms: record.expires_at_ms,
                superseded: record.superseded,
            })
            .collect();
        self.verify_lock_identity()?;
        Ok(StoppedFileReceipt {
            schema: "tsoracle.stopped-file-state",
            schema_version: 1,
            producer: Producer {
                name: env!("CARGO_PKG_NAME"),
                version: env!("CARGO_PKG_VERSION"),
            },
            driver: "file",
            epoch: 0,
            units: Units {
                reservation_and_lease_upper_bound: "unix-epoch physical milliseconds, inclusive, all logical values reserved",
                lease_expiry: "absolute unix-epoch milliseconds on oracle server clock",
                lease_ttl: "milliseconds",
                dense_high_water: "next unallocated ordinal, exclusive upper bound of reserved ordinals",
                holder: "opaque bytes encoded as lowercase hexadecimal",
            },
            files,
            committed_reservation_high_water_physical_ms: high_water,
            maximum_lease_upper_bound_physical_ms: leases
                .iter()
                .map(|r| r.ts_upper_bound_physical_ms)
                .max(),
            maximum_lease_expires_at_unix_ms: leases.iter().map(|r| r.expires_at_unix_ms).max(),
            leases,
            dense_next_unallocated: dense,
            dense_cardinality_cap: cap,
        })
    }

    fn verify_lock_identity(&self) -> Result<(), ExportError> {
        let path = self.dir.join(LOCK_FILE);
        let entry = fs::symlink_metadata(&path).map_err(|source| io_error(&path, source))?;
        let held = self
            .lock
            .metadata()
            .map_err(|source| io_error(&path, source))?;
        if !entry.is_file() || !held.is_file() {
            return invalid("LOCK must remain a regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            if held.nlink() != 1
                || entry.nlink() != 1
                || held.dev() != entry.dev()
                || held.ino() != entry.ino()
            {
                return invalid("LOCK identity changed while ownership was held");
            }
        }
        #[cfg(windows)]
        {
            let current = open_regular(&path)?;
            if windows_identity(&self.lock).map_err(|source| io_error(&path, source))?
                != windows_identity(&current).map_err(|source| io_error(&path, source))?
            {
                return invalid("LOCK identity changed while ownership was held");
            }
        }
        #[cfg(not(any(unix, windows)))]
        return invalid("native file identity is unavailable on this platform");
        Ok(())
    }
}

fn io_error(path: &Path, source: std::io::Error) -> ExportError {
    ExportError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn magic(bytes: &'static [u8; 4]) -> Result<&'static str, ExportError> {
    std::str::from_utf8(bytes)
        .map_err(|_| ExportError::Invalid("record magic must be UTF-8".into()))
}

fn inventory(
    name: &str,
    bytes: Option<&[u8]>,
    format: &'static str,
    format_version: Option<u8>,
) -> FileInventoryEntry {
    FileInventoryEntry {
        path: name.to_string(),
        present: bytes.is_some(),
        size_bytes: bytes.map(|bytes| bytes.len() as u64),
        sha256: bytes.map(|bytes| hex(&Sha256::digest(bytes))),
        format,
        format_version,
    }
}

fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, ExportError> {
    match read_bounded(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ExportError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn invalid<T>(message: &str) -> Result<T, ExportError> {
    Err(ExportError::Invalid(message.to_string()))
}

fn open_regular(path: &Path) -> Result<File, ExportError> {
    if !fs::symlink_metadata(path)
        .map_err(|source| io_error(path, source))?
        .file_type()
        .is_file()
    {
        return invalid("state entries must be regular files without symlinks");
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options
        .open(path)
        .map_err(|source| io_error(path, source))?;
    let metadata = file.metadata().map_err(|source| io_error(path, source))?;
    if !metadata.is_file() {
        return invalid("state entries must be regular files");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return invalid("state entries must have exactly one hard link");
        }
    }
    Ok(file)
}

fn read_bounded(path: &Path) -> Result<Vec<u8>, ExportError> {
    let file = open_regular(path)?;
    if file
        .metadata()
        .map_err(|source| io_error(path, source))?
        .len()
        > MAX_FILE_BYTES
    {
        return invalid("state file exceeds the 64 MiB export limit");
    }
    let mut bytes = Vec::new();
    file.take(MAX_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| io_error(path, source))?;
    if bytes.len() as u64 > MAX_FILE_BYTES {
        return invalid("state file exceeds the 64 MiB export limit");
    }
    Ok(bytes)
}

#[cfg(windows)]
fn windows_identity(file: &File) -> std::io::Result<(u32, [u32; 2])> {
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileInformation {
        _attributes: u32,
        _times: [u32; 6],
        volume: u32,
        _size: [u32; 2],
        links: u32,
        index: [u32; 2],
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        #[link_name = "GetFileInformationByHandle"]
        fn get_file_information_by_handle(
            handle: *mut std::ffi::c_void,
            information: *mut FileInformation,
        ) -> i32;
    }
    let mut information = FileInformation {
        _attributes: 0,
        _times: [0; 6],
        volume: 0,
        _size: [0; 2],
        links: 0,
        index: [0; 2],
    };
    // SAFETY: the live handle and writable buffer match BY_HANDLE_FILE_INFORMATION's C layout.
    if unsafe { get_file_information_by_handle(file.as_raw_handle(), &mut information) } == 0 {
        return Err(std::io::Error::last_os_error());
    }
    if information.links != 1 {
        return Err(std::io::Error::other(
            "LOCK must have exactly one hard link",
        ));
    }
    Ok((information.volume, information.index))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // String formatting cannot fail; this avoids a second byte encoding dependency.
        let _ = write!(result, "{byte:02x}");
    }
    result
}

#[cfg(all(test, feature = "stopped-state-export"))]
mod tests {
    use super::*;
    use crate::FileDriver;
    use crate::test_support::{populate, snapshot};
    use tsoracle_consensus::ConsensusDriver;
    use tsoracle_core::{Epoch, LeaseRecord, PHYSICAL_MS_MAX, SeqKey};

    fn fixture(dir: &Path) {
        File::create(dir.join("LOCK")).unwrap();
        fs::write(dir.join("state"), record::encode(500)).unwrap();
        fs::write(
            dir.join("dense"),
            dense_record::encode(&BTreeMap::new(), 10),
        )
        .unwrap();
        fs::write(dir.join("leases"), lease_record::encode(&[])).unwrap();
    }

    fn export(dir: &Path) -> Result<StoppedFileReceipt, ExportError> {
        StoppedFileState::open(dir)?.receipt()
    }

    fn lease(id: u64, superseded: bool) -> LeaseRecord {
        LeaseRecord {
            lease_id: id,
            holder: vec![0, 0xff],
            holder_epoch: id,
            ttl_ms: 5000,
            ts_upper_bound: id,
            expires_at_ms: if superseded { u64::MAX } else { 1 },
            superseded,
        }
    }

    #[tokio::test]
    async fn exports_producer_state_and_all_retained_leases_without_clock_filtering() {
        let dir = tempfile::tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        populate(driver.as_ref()).await;
        driver
            .persist_leases(&[lease(100, true), lease(200, false)], Epoch::ZERO)
            .await
            .unwrap();
        drop(driver);
        let before = snapshot(dir.path());
        let receipt = export(dir.path()).unwrap();
        assert_eq!(receipt.schema, "tsoracle.stopped-file-state");
        assert_eq!(receipt.schema_version, 1);
        assert_eq!(receipt.producer.name, "tsoracle-driver-file");
        assert_eq!(receipt.producer.version, env!("CARGO_PKG_VERSION"));
        assert_eq!(receipt.driver, "file");
        assert_eq!(receipt.epoch, 0);
        assert_eq!(receipt.committed_reservation_high_water_physical_ms, 500);
        assert_eq!(receipt.dense_next_unallocated["orders"], 7);
        assert_eq!(receipt.dense_cardinality_cap, 10_000);
        assert_eq!(receipt.leases.len(), 2);
        assert_eq!(receipt.leases[0].holder_hex, "00ff");
        assert_eq!(receipt.leases[0].holder_epoch, 100);
        assert_eq!(receipt.leases[0].ttl_ms, 5000);
        assert!(receipt.leases[0].superseded);
        assert!(!receipt.leases[1].superseded);
        assert_eq!(receipt.maximum_lease_upper_bound_physical_ms, Some(200));
        assert_eq!(receipt.maximum_lease_expires_at_unix_ms, Some(u64::MAX));
        assert_eq!(receipt.files.len(), 4);
        for file in &receipt.files {
            let bytes = &before[std::ffi::OsStr::new(&file.path)];
            assert!(file.present);
            assert_eq!(file.size_bytes, Some(bytes.len() as u64));
            assert_eq!(file.sha256, Some(hex(&Sha256::digest(bytes))));
            match file.path.as_str() {
                "state" => assert_eq!((file.format, file.format_version), ("TSOR", Some(1))),
                "dense" => assert_eq!((file.format, file.format_version), ("TSOD", Some(1))),
                "leases" => assert_eq!((file.format, file.format_version), ("TSOO", Some(1))),
                "LOCK" => assert_eq!(
                    file.sha256,
                    Some(
                        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                            .to_string()
                    )
                ),
                _ => panic!("unexpected file"),
            }
        }
        assert_eq!(snapshot(dir.path()), before);
    }

    #[test]
    fn exports_known_empty_sets_and_maximum_floors() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let empty = export(dir.path()).unwrap();
        assert!(empty.leases.is_empty());
        assert!(empty.dense_next_unallocated.is_empty());
        assert_eq!(empty.maximum_lease_upper_bound_physical_ms, None);
        assert_eq!(empty.maximum_lease_expires_at_unix_ms, None);
        fs::write(dir.path().join("state"), record::encode(PHYSICAL_MS_MAX)).unwrap();
        let dense = BTreeMap::from([("exhausted".to_string(), u64::MAX)]);
        fs::write(dir.path().join("dense"), dense_record::encode(&dense, 1)).unwrap();
        fs::write(
            dir.path().join("leases"),
            lease_record::encode(&[lease(PHYSICAL_MS_MAX, true)]),
        )
        .unwrap();
        let maximum = export(dir.path()).unwrap();
        assert_eq!(
            maximum.committed_reservation_high_water_physical_ms,
            PHYSICAL_MS_MAX
        );
        assert_eq!(maximum.dense_next_unallocated["exhausted"], u64::MAX);
        assert_eq!(
            maximum.maximum_lease_upper_bound_physical_ms,
            Some(PHYSICAL_MS_MAX)
        );
    }

    #[test]
    fn refuses_missing_required_files_without_mutation() {
        for name in [LOCK_FILE, STATE_FILE] {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            fs::remove_file(dir.path().join(name)).unwrap();
            let before = snapshot(dir.path());
            assert!(export(dir.path()).is_err(), "{name}");
            assert_eq!(snapshot(dir.path()), before, "{name}");
        }
    }

    #[test]
    fn refuses_absent_and_lazy_uninitialized_directories() {
        let parent = tempfile::tempdir().unwrap();
        let missing = parent.path().join("missing");
        assert!(export(&missing).is_err());
        assert!(!missing.exists());
        let driver = FileDriver::open_or_init(parent.path()).unwrap();
        drop(driver);
        let before = snapshot(parent.path());
        assert!(export(parent.path()).is_err());
        assert_eq!(snapshot(parent.path()), before);
    }

    #[test]
    fn refuses_corruption_truncation_versions_and_trailing_bytes() {
        for name in ["state", "dense", "leases"] {
            for mode in 0..4 {
                let dir = tempfile::tempdir().unwrap();
                fixture(dir.path());
                let path = dir.path().join(name);
                let mut bytes = fs::read(&path).unwrap();
                match mode {
                    0 => bytes[0] ^= 1,
                    1 => {
                        bytes.pop();
                    }
                    2 => bytes[4] = 99,
                    _ => bytes.push(0),
                }
                fs::write(path, bytes).unwrap();
                let before = snapshot(dir.path());
                assert!(export(dir.path()).is_err(), "{name} mode {mode}");
                assert_eq!(snapshot(dir.path()), before);
            }
        }
    }

    #[test]
    fn refuses_checksum_corruption_in_every_record() {
        for name in ["state", "dense", "leases"] {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            let path = dir.path().join(name);
            let mut bytes = fs::read(&path).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            fs::write(path, bytes).unwrap();
            let before = snapshot(dir.path());
            assert!(export(dir.path()).is_err());
            assert_eq!(snapshot(dir.path()), before);
        }
    }

    #[test]
    fn refuses_checksummed_inconsistent_state() {
        for case in 0..7 {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            match case {
                0 => fs::write(
                    dir.path().join("state"),
                    record::encode(PHYSICAL_MS_MAX + 1),
                )
                .unwrap(),
                1 => fs::write(
                    dir.path().join("leases"),
                    lease_record::encode(&[lease(501, false)]),
                )
                .unwrap(),
                2..=4 => {
                    let mut record = lease(100, false);
                    match case {
                        2 => record.lease_id = 101,
                        3 => record.holder.clear(),
                        _ => record.ttl_ms = 0,
                    }
                    fs::write(dir.path().join("leases"), lease_record::encode(&[record])).unwrap();
                }
                _ => {
                    let map = BTreeMap::from([(if case == 5 { "" } else { "a" }.to_string(), 1)]);
                    fs::write(
                        dir.path().join("dense"),
                        dense_record::encode(&map, if case == 5 { 10 } else { 0 }),
                    )
                    .unwrap();
                }
            }
            let before = snapshot(dir.path());
            assert!(export(dir.path()).is_err(), "case {case}");
            assert_eq!(snapshot(dir.path()), before);
        }
    }

    #[test]
    fn refuses_temporary_unknown_files_and_nonempty_lock() {
        for name in ["state.tmp", "dense.tmp", "leases.tmp", "unknown", "LOCK"] {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            fs::write(dir.path().join(name), b"incomplete").unwrap();
            let before = snapshot(dir.path());
            assert!(export(dir.path()).is_err(), "{name}");
            assert_eq!(snapshot(dir.path()), before);
        }
    }

    #[test]
    fn lock_excludes_driver_and_exporters_until_guard_drop() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        assert!(matches!(
            StoppedFileState::open(dir.path()),
            Err(ExportError::Driver(FileDriverError::AlreadyLocked { .. }))
        ));
        drop(driver);
        let before = snapshot(dir.path());
        let guard = StoppedFileState::open(dir.path()).unwrap();
        guard.receipt().unwrap();
        guard.receipt().unwrap();
        assert!(matches!(
            FileDriver::open_or_init(dir.path()),
            Err(FileDriverError::AlreadyLocked { .. })
        ));
        assert!(StoppedFileState::open(dir.path()).is_err());
        assert_eq!(snapshot(dir.path()), before);
        drop(guard);
        assert!(FileDriver::open_or_init(dir.path()).is_ok());
    }

    #[test]
    fn refusal_releases_native_ownership() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        fs::write(dir.path().join("state"), b"bad").unwrap();
        assert!(export(dir.path()).is_err());
        fs::write(dir.path().join("state"), record::encode(500)).unwrap();
        assert!(export(dir.path()).is_ok());
    }

    #[test]
    fn refuses_oversized_files() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        OpenOptions::new()
            .write(true)
            .open(dir.path().join("dense"))
            .unwrap()
            .set_len(MAX_FILE_BYTES + 1)
            .unwrap();
        assert!(export(dir.path()).is_err());
        assert_eq!(
            fs::metadata(dir.path().join("dense")).unwrap().len(),
            MAX_FILE_BYTES + 1
        );
    }

    #[cfg(unix)]
    #[test]
    fn refuses_symlinks_hardlinks_and_nonregular_files() {
        use std::os::unix::fs::symlink;
        for name in FILES {
            for mode in 0..3 {
                let dir = tempfile::tempdir().unwrap();
                fixture(dir.path());
                let outside = tempfile::tempdir().unwrap();
                let source = outside.path().join("source");
                fs::write(&source, fs::read(dir.path().join(name)).unwrap()).unwrap();
                fs::remove_file(dir.path().join(name)).unwrap();
                match mode {
                    0 => symlink(&source, dir.path().join(name)).unwrap(),
                    1 => fs::hard_link(&source, dir.path().join(name)).unwrap(),
                    _ => fs::create_dir(dir.path().join(name)).unwrap(),
                }
                assert!(export(dir.path()).is_err(), "{name} mode {mode}");
            }
        }
        let parent = tempfile::tempdir().unwrap();
        let source = parent.path().join("source");
        fs::create_dir(&source).unwrap();
        fixture(&source);
        symlink(&source, parent.path().join("linked")).unwrap();
        assert!(export(&parent.path().join("linked")).is_err());
    }

    #[tokio::test]
    async fn guard_covers_copy_and_restored_dense_floor() {
        let source = tempfile::tempdir().unwrap();
        fixture(source.path());
        let driver = FileDriver::open_or_init(source.path()).unwrap();
        driver
            .advance_dense(&SeqKey::try_new("orders").unwrap(), 7, Epoch::ZERO)
            .await
            .unwrap();
        drop(driver);
        let guard = StoppedFileState::open(source.path()).unwrap();
        let receipt = guard.receipt().unwrap();
        let destination = tempfile::tempdir().unwrap();
        for file in &receipt.files {
            if !file.present {
                continue;
            }
            if file.path == LOCK_FILE {
                File::create(destination.path().join(LOCK_FILE)).unwrap();
                continue;
            }
            fs::copy(
                source.path().join(&file.path),
                destination.path().join(&file.path),
            )
            .unwrap();
        }
        assert!(FileDriver::open_or_init(source.path()).is_err());
        let restored = export(destination.path()).unwrap();
        assert_eq!(
            restored.dense_next_unallocated,
            receipt.dense_next_unallocated
        );
        let driver = FileDriver::open_or_init(destination.path()).unwrap();
        assert_eq!(
            driver.load_high_water().await.unwrap(),
            receipt.committed_reservation_high_water_physical_ms
        );
        assert_eq!(
            driver
                .advance_dense(&SeqKey::try_new("orders").unwrap(), 1, Epoch::ZERO)
                .await
                .unwrap(),
            7
        );
        drop(guard);
    }

    #[tokio::test]
    async fn exports_renewed_lease_and_reloads_it() {
        let dir = tempfile::tempdir().unwrap();
        let driver = FileDriver::open_or_init(dir.path()).unwrap();
        driver.persist_high_water(6000, Epoch::ZERO).await.unwrap();
        let mut renewed = lease(1000, false);
        renewed.ts_upper_bound = 6000;
        driver
            .persist_leases(&[renewed], Epoch::ZERO)
            .await
            .unwrap();
        drop(driver);
        let receipt = export(dir.path()).unwrap();
        assert_eq!(receipt.leases[0].lease_id, 1000);
        assert_eq!(receipt.leases[0].ts_upper_bound_physical_ms, 6000);
        assert!(FileDriver::open_or_init(dir.path()).is_ok());
    }

    #[tokio::test]
    async fn exports_timestamp_only_and_seeded_directories_without_creating_lazy_files() {
        for seeded in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            if seeded {
                FileDriver::init_seeded(dir.path(), 500).unwrap();
            } else {
                let driver = FileDriver::open_or_init(dir.path()).unwrap();
                driver.persist_high_water(500, Epoch::ZERO).await.unwrap();
            }
            let before = snapshot(dir.path());
            let receipt = export(dir.path()).unwrap();
            assert!(receipt.leases.is_empty());
            assert!(receipt.dense_next_unallocated.is_empty());
            assert_eq!(receipt.dense_cardinality_cap, DEFAULT_DENSE_CARDINALITY_CAP);
            for name in [DENSE_FILE, LEASES_FILE] {
                let entry = receipt
                    .files
                    .iter()
                    .find(|entry| entry.path == name)
                    .unwrap();
                assert!(!entry.present);
                assert_eq!(entry.size_bytes, None);
                assert_eq!(entry.sha256, None);
            }
            assert_eq!(snapshot(dir.path()), before);
        }
    }

    #[test]
    fn exports_each_independently_absent_lazy_record() {
        for name in [DENSE_FILE, LEASES_FILE] {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            fs::remove_file(dir.path().join(name)).unwrap();
            let before = snapshot(dir.path());
            let receipt = export(dir.path()).unwrap();
            assert!(
                !receipt
                    .files
                    .iter()
                    .find(|entry| entry.path == name)
                    .unwrap()
                    .present
            );
            assert!(receipt.leases.is_empty());
            assert!(receipt.dense_next_unallocated.is_empty());
            assert_eq!(snapshot(dir.path()), before);
        }
    }

    #[test]
    fn certifies_using_its_own_locked_descriptor_repeatedly() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let guard = StoppedFileState::open(dir.path()).unwrap();
        for _ in 0..2 {
            let receipt = guard.receipt().unwrap();
            assert_eq!(receipt.files[0].size_bytes, Some(0));
        }
    }

    #[cfg(unix)]
    #[test]
    fn refuses_replaced_lock_even_when_replacement_has_a_live_owner() {
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let guard = StoppedFileState::open(dir.path()).unwrap();
        let old_lock = tempfile::tempdir().unwrap();
        fs::rename(dir.path().join(LOCK_FILE), old_lock.path().join(LOCK_FILE)).unwrap();
        File::create(dir.path().join(LOCK_FILE)).unwrap();
        let owner = FileDriver::open_or_init(dir.path()).unwrap();
        assert!(guard.receipt().is_err());
        drop(owner);
    }

    #[cfg(unix)]
    #[test]
    fn opens_read_only_lock_without_write_access() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        fixture(dir.path());
        let path = dir.path().join(LOCK_FILE);
        fs::set_permissions(&path, fs::Permissions::from_mode(0o444)).unwrap();
        let guard = StoppedFileState::open(dir.path()).unwrap();
        // SAFETY: the guard owns a live descriptor and F_GETFL takes no extra argument.
        let flags = unsafe { libc::fcntl(guard.lock.as_raw_fd(), libc::F_GETFL) };
        assert_ne!(flags, -1);
        assert_eq!(flags & libc::O_ACCMODE, libc::O_RDONLY);
        guard.receipt().unwrap();
    }

    #[test]
    fn decode_errors_keep_their_record_type_and_path() {
        for name in [STATE_FILE, DENSE_FILE, LEASES_FILE] {
            let dir = tempfile::tempdir().unwrap();
            fixture(dir.path());
            let path = dir.path().join(name);
            fs::write(&path, b"bad").unwrap();
            let error = export(dir.path()).unwrap_err();
            match (name, error) {
                (STATE_FILE, ExportError::StateDecode { path: found, .. })
                | (DENSE_FILE, ExportError::DenseDecode { path: found, .. })
                | (LEASES_FILE, ExportError::LeaseDecode { path: found, .. }) => {
                    assert_eq!(found, path)
                }
                (_, other) => panic!("unexpected error: {other:?}"),
            }
        }
        let parent = tempfile::tempdir().unwrap();
        let missing = parent.path().join("missing");
        assert!(
            matches!(StoppedFileState::open(&missing), Err(ExportError::Io { path, .. }) if path == missing)
        );
        fixture(parent.path());
        fs::remove_file(parent.path().join(LOCK_FILE)).unwrap();
        assert!(
            matches!(StoppedFileState::open(parent.path()), Err(ExportError::Io { path, .. }) if path == parent.path().join(LOCK_FILE))
        );
        File::create(parent.path().join(LOCK_FILE)).unwrap();
        fs::remove_file(parent.path().join(STATE_FILE)).unwrap();
        assert!(
            matches!(export(parent.path()), Err(ExportError::Io { path, .. }) if path == parent.path().join(STATE_FILE))
        );
    }
}
