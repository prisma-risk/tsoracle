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

//! Filled in commit 3.

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaxosPeer {
    pub node_id: u64,
    pub endpoint: String,
}

#[must_use]
pub fn encode_epoch(_ballot: omnipaxos::ballot_leader_election::Ballot) -> tsoracle_core::Epoch {
    tsoracle_core::Epoch(0)
}

#[must_use]
pub fn decode_epoch(_epoch: tsoracle_core::Epoch) -> (u32, u32, u64) {
    (0, 0, 0)
}
