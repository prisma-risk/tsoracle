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

use std::fs;
use tempfile::tempdir;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::Epoch;
use tsoracle_driver_file::{FileDriver, FileDriverError};

#[tokio::test]
async fn corrupted_crc_is_rejected() {
    let dir = tempdir().unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    driver.persist_high_water(12345, Epoch::ZERO).await.unwrap();
    drop(driver);

    // Corrupt the high-water field after the magic but before the CRC.
    let mut bytes = fs::read(dir.path().join("state")).unwrap();
    bytes[5] ^= 0xFF;
    fs::write(dir.path().join("state"), bytes).unwrap();

    let err = FileDriver::open_or_init(dir.path()).unwrap_err();
    assert!(matches!(err, FileDriverError::Decode(_)));
}

#[tokio::test]
async fn left_over_tmp_file_is_ignored() {
    let dir = tempdir().unwrap();
    fs::write(dir.path().join("state.tmp"), b"garbage").unwrap();
    let driver = FileDriver::open_or_init(dir.path()).unwrap();
    assert_eq!(driver.load_high_water().await.unwrap(), 0);
    // First persist should still work, overwriting state.tmp.
    let actual = driver.persist_high_water(42, Epoch::ZERO).await.unwrap();
    assert_eq!(actual, 42);
}

#[tokio::test]
async fn open_init_creates_missing_dir() {
    let parent = tempdir().unwrap();
    let nested = parent.path().join("a").join("b").join("c");
    let driver = FileDriver::open_or_init(&nested).unwrap();
    assert_eq!(driver.load_high_water().await.unwrap(), 0);
}
