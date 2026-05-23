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

use std::{sync::Arc, time::Duration};
use tokio::time::timeout;
use tsoracle_consensus::ConsensusDriver;
use tsoracle_core::{Epoch, testing::MockClock};
use tsoracle_server::{
    Server, ServerError, ServingState,
    test_fakes::{FaultKind, FaultyDriver, InMemoryDriver},
};

/// Drive a leader transition to completion and return when `state_rx` reports
/// `Serving`. Shared by the regression tests below.
async fn wait_until_serving(state_rx: &mut tokio::sync::watch::Receiver<ServingState>) {
    loop {
        let snapshot = state_rx.borrow_and_update().clone();
        if matches!(snapshot, ServingState::Serving) {
            return;
        }
        state_rx.changed().await.unwrap();
    }
}

#[tokio::test]
async fn leader_watch_persists_fence_before_serving() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(10_000));

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .failover_advance(Duration::from_secs(2))
            .build()
            .unwrap(),
    );

    // Seed the driver's prior-leader high-water.
    driver.persist_high_water(5_000, Epoch(0)).await.unwrap();
    assert_eq!(driver.current_high_water(), 5_000);

    // Spawn the leader-watch task and trigger a leader transition.
    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    // Wait until serving = true.
    let mut state_rx = server.state_rx.clone();
    wait_until_serving(&mut state_rx).await;

    // max(prior_max + 1 = 5_001, now = 10_000) + failover_advance = 2_000 = 12_000.
    // (The +1 in the serving floor doesn't change the outcome here because now > prior_max.)
    assert!(
        driver.current_high_water() >= 12_000,
        "actual: {}",
        driver.current_high_water()
    );

    watch_handle.abort();
}

/// Regression test for the failover-fence fencepost bug.
///
/// `prior_high_water` is an *inclusive* high-water — the prior leader could
/// have issued a timestamp at `Timestamp::pack(prior_max, LOGICAL_MAX)`. The
/// new leader must therefore persist a strictly greater value before it can
/// safely serve. With `failover_advance = 0`, the only thing keeping the
/// persisted bound above `prior_max` is the `+1` in the serving-floor
/// computation; without it the driver records `prior_max` exactly and the
/// new leader's first grant collides with the prior leader's last.
#[tokio::test]
async fn fence_strictly_above_prior_high_water_when_prior_exceeds_now() {
    let driver = Arc::new(InMemoryDriver::new());
    // now < prior_high_water: this is the dangerous case (clock-step-back or
    // a slow new leader). Set advance=0 so the floor's +1 is the *only* thing
    // separating the new bound from the prior leader's inclusive maximum.
    let clock = Arc::new(MockClock::new(1_000));
    let prior_max = 50_000u64;

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .failover_advance(Duration::from_secs(0))
            .build()
            .unwrap(),
    );

    driver
        .persist_high_water(prior_max, Epoch(0))
        .await
        .unwrap();

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    let mut state_rx = server.state_rx.clone();
    wait_until_serving(&mut state_rx).await;

    let persisted = driver.current_high_water();
    assert!(
        persisted > prior_max,
        "new leader persisted high_water = {persisted}, but prior_high_water = {prior_max}; \
         the persisted value must be STRICTLY above the prior inclusive high-water"
    );

    watch_handle.abort();
}

/// Regression test for the actual behavioral invariant the fence protects:
/// the new leader's first issued timestamp must be strictly greater than
/// any timestamp the prior leader could have served.
///
/// The prior leader's largest possible timestamp is
/// `Timestamp::pack(prior_max, LOGICAL_MAX)`. With the buggy code, the new
/// leader initializes `next_physical_ms = prior_max` and `next_logical = 0`,
/// so the first grant is `Timestamp::pack(prior_max, 0)` — strictly LESS than
/// what the prior leader could have issued. This test fails in that case
/// regardless of `failover_advance`, because the bug is in the floor, not
/// the ceiling.
#[tokio::test]
async fn first_grant_strictly_above_prior_high_water_when_prior_exceeds_now() {
    use tsoracle_core::{LOGICAL_MAX, Timestamp};

    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(1_000));
    let prior_max = 50_000u64;

    // failover_advance > 0 here — the bug must still surface, because the
    // bug is that the *floor* (not the ceiling) equals prior_max. The
    // advance only lifts the ceiling above the floor; it does nothing to
    // prevent the first grant from landing at prior_max.
    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .failover_advance(Duration::from_secs(2))
            .build()
            .unwrap(),
    );

    driver
        .persist_high_water(prior_max, Epoch(0))
        .await
        .unwrap();

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    let mut state_rx = server.state_rx.clone();
    wait_until_serving(&mut state_rx).await;

    let grant = server
        .try_grant_for_tests(1)
        .expect("allocator should serve immediately after leadership gain");

    let prior_leader_max_ts = Timestamp::pack(prior_max, LOGICAL_MAX);
    assert!(
        grant.first() > prior_leader_max_ts,
        "first grant = (physical_ms={}, logical={}); prior leader could have issued up to \
         (physical_ms={}, logical={}). New leader's first timestamp must be strictly greater.",
        grant.physical_ms,
        grant.logical_start,
        prior_max,
        LOGICAL_MAX,
    );

    watch_handle.abort();
}

/// A transient driver error during the fence is recoverable: the new leader
/// must retry rather than tear the server down. Models a momentary quorum
/// blip in the volatile post-election window. Without recovery the fence's
/// `?` terminates the watch task and the server never reaches `Serving`.
#[tokio::test]
async fn fence_retries_transient_persist_error_then_serves() {
    let driver = Arc::new(FaultyDriver::new());
    let clock = Arc::new(MockClock::new(10_000));

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .failover_advance(Duration::from_secs(2))
            .build()
            .unwrap(),
    );

    // The first persist of the fence fails transiently; the retry must succeed.
    driver.fail_next_persists(1, FaultKind::Transient);

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    let mut state_rx = server.state_rx.clone();
    timeout(Duration::from_secs(5), wait_until_serving(&mut state_rx))
        .await
        .expect("fence must recover from a transient persist error and reach Serving");

    // It really retried (more than one persist attempt) and fenced above the floor.
    assert!(
        driver.persist_call_count() >= 2,
        "expected a retry (>=2 persist calls), saw {}",
        driver.persist_call_count()
    );
    assert!(driver.current_high_water() >= 12_000);

    watch_handle.abort();
}

/// A `NotLeader` error during the fence means leadership moved under us — a
/// failover flap. The watch task must NOT terminate; it steps down to
/// `NotServing` and fences cleanly on the next leadership event.
#[tokio::test]
async fn fence_steps_down_on_not_leader_then_serves_next_election() {
    let driver = Arc::new(FaultyDriver::new());
    let clock = Arc::new(MockClock::new(10_000));

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .failover_advance(Duration::from_secs(2))
            .build()
            .unwrap(),
    );

    // The first leadership gain races with a flap: persist returns NotLeader.
    driver.fail_next_persists(1, FaultKind::NotLeader);

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    // First election: the fence hits NotLeader. Wait until that failing persist
    // is observed so the next election is a distinct leadership event.
    driver.become_leader(Epoch(1));
    timeout(Duration::from_secs(5), async {
        while driver.persist_call_count() < 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("fence should have attempted (and failed) the first persist");

    // A NotLeader fence error must not be fatal — the watch task stays alive.
    assert!(
        !watch_handle.is_finished(),
        "watch task terminated on a NotLeader fence error; it must step down and await the next election"
    );

    // Re-won leadership: the fence must now complete and serve.
    driver.become_leader(Epoch(2));
    let mut state_rx = server.state_rx.clone();
    timeout(Duration::from_secs(5), wait_until_serving(&mut state_rx))
        .await
        .expect("fence must serve after re-winning leadership");

    watch_handle.abort();
}

/// A permanent driver error stays fatal: the watch task terminates with the
/// error (and `into_router` poisons serving state). Guards the fail-safe for
/// unrecoverable faults so the transient-recovery change does not silently
/// swallow corruption-class errors.
#[tokio::test]
async fn fence_permanent_error_terminates_watch() {
    let driver = Arc::new(FaultyDriver::new());
    let clock = Arc::new(MockClock::new(10_000));

    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .build()
            .unwrap(),
    );

    driver.fail_next_persists(1, FaultKind::Permanent);

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_leader(Epoch(1));

    let result = timeout(Duration::from_secs(5), watch_handle)
        .await
        .expect("watch task must terminate on a permanent error")
        .expect("watch task panicked");

    assert!(
        matches!(result, Err(ServerError::Consensus(_))),
        "expected fatal ServerError::Consensus, got {result:?}"
    );
}

#[tokio::test]
async fn leader_watch_propagates_follower_epoch_into_serving_state() {
    let driver = Arc::new(InMemoryDriver::new());
    let clock = Arc::new(MockClock::new(10_000));
    let server = Arc::new(
        Server::builder()
            .consensus_driver(driver.clone())
            .clock(clock.clone())
            .build()
            .unwrap(),
    );

    let watch_server = server.clone();
    let watch_handle = tokio::spawn(async move { watch_server.run_leader_watch_for_tests().await });

    driver.become_follower_with_epoch(Some("http://leader:9000".into()), Some(Epoch(7)));

    let mut state_rx = server.state_rx.clone();
    let snapshot = timeout(Duration::from_secs(1), async {
        loop {
            let current = state_rx.borrow_and_update().clone();
            if let ServingState::NotServing {
                leader_epoch: Some(_),
                ..
            } = current
            {
                return current;
            }
            state_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("ServingState should report a follower epoch");

    match snapshot {
        ServingState::NotServing {
            leader_endpoint,
            leader_epoch,
        } => {
            assert_eq!(leader_endpoint.as_deref(), Some("http://leader:9000"));
            assert_eq!(leader_epoch, Some(Epoch(7)));
        }
        other => panic!("expected NotServing, got {other:?}"),
    }

    watch_handle.abort();
}
