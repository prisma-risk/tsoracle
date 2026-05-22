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
//! Coalesces concurrent waiters into one outgoing GetTs RPC.
//!
//! The driver never retains pre-fetched timestamps. Each waiter receives
//! timestamps that the server allocated after that waiter enqueued — never
//! from a prior RPC's leftover range. This is the freshness invariant the
//! library promises strict-consistency callers.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, sleep_until};
use tsoracle_core::Timestamp;

use crate::MAX_TIMESTAMPS_PER_RPC;
use crate::error::ClientError;

/// Bound on the waiter queue. A slow server combined with a fast caller
/// must not grow this without limit; once full, `Driver::request` awaits
/// via `Sender::send().await`, propagating backpressure to callers.
///
/// Each `Waiter` is small (~32 bytes), so 4096 caps the queue at ~128 KB
/// regardless of how aggressive the producers are.
const QUEUE_CAPACITY: usize = 4096;

pub(crate) struct Waiter {
    pub count: u32,
    pub respond: oneshot::Sender<Result<Vec<Timestamp>, ClientError>>,
}

pub(crate) struct Driver {
    tx: mpsc::Sender<Waiter>,
}

type RpcFn = Arc<
    dyn Fn(u32) -> futures::future::BoxFuture<'static, Result<Vec<Timestamp>, ClientError>>
        + Send
        + Sync,
>;

/// One (expected_count, rpc_result, waiters-for-this-chunk) entry. The
/// driver task spawns a sub-task that fills a `Vec<ChunkResult>` one chunk
/// at a time, then delivers each chunk's result on the parent task.
type ChunkResult = (u32, Result<Vec<Timestamp>, ClientError>, VecDeque<Waiter>);
type BatchHandle = tokio::task::JoinHandle<Vec<ChunkResult>>;

impl Driver {
    pub fn spawn<F>(rpc: F, flush_interval: Duration) -> Self
    where
        F: Fn(u32) -> futures::future::BoxFuture<'static, Result<Vec<Timestamp>, ClientError>>
            + Send
            + Sync
            + 'static,
    {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        tokio::spawn(driver_task(Arc::new(rpc), rx, flush_interval));
        Driver { tx }
    }

    pub async fn request(&self, count: u32) -> Result<Vec<Timestamp>, ClientError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.tx
            .send(Waiter {
                count,
                respond: resp_tx,
            })
            .await
            .map_err(|_| ClientError::NoReachableEndpoints)?;
        resp_rx
            .await
            .map_err(|_| ClientError::NoReachableEndpoints)?
    }
}

async fn driver_task(rpc: RpcFn, mut rx: mpsc::Receiver<Waiter>, flush_interval: Duration) {
    let mut queue: VecDeque<Waiter> = VecDeque::new();
    let mut first_arrival: Option<Instant> = None;
    let mut in_flight: Option<BatchHandle> = None;

    loop {
        if let Some(handle) = in_flight.as_mut() {
            tokio::select! {
                biased;
                completed = handle => {
                    in_flight = None;
                    set_in_flight_gauge(0);
                    if let Ok(chunks) = completed {
                        for (expected, result, mut waiters) in chunks {
                            deliver(&mut waiters, result, expected);
                        }
                    }
                }
                next = rx.recv() => {
                    match next {
                        Some(w) => enqueue(&mut queue, &mut first_arrival, w),
                        None => return,
                    }
                }
            }
        } else {
            if queue.is_empty() {
                match rx.recv().await {
                    Some(w) => enqueue(&mut queue, &mut first_arrival, w),
                    None => return,
                }
            }
            // `first_arrival` is `Some` whenever the queue is non-empty —
            // `enqueue` sets it on every appended waiter, and the
            // `queue.is_empty()` branch above guarantees one was accepted. The
            // `if let` keeps that invariant explicit: if a future refactor
            // breaks it, the driver skips the wait and falls through to the
            // empty-`chunk_queue` `continue` rather than panicking.
            if flush_interval > Duration::ZERO
                && let Some(first) = first_arrival
            {
                let deadline = first + flush_interval;
                loop {
                    tokio::select! {
                        biased;
                        _ = sleep_until(deadline) => break,
                        next = rx.recv() => {
                            match next {
                                Some(w) => enqueue(&mut queue, &mut first_arrival, w),
                                None => return,
                            }
                        }
                    }
                }
            }
            first_arrival = None;

            let chunks = chunk_queue(&mut queue);
            set_queue_depth_gauge(queue.len());
            if chunks.is_empty() {
                // Every waiter was rejected inline (oversize/zero counts).
                continue;
            }
            let rpc_fn = rpc.clone();
            in_flight = Some(tokio::spawn(
                async move { run_chunks(rpc_fn, chunks).await },
            ));
            set_in_flight_gauge(1);
        }
    }
}

/// Drain `queue` into one or more (total, waiters) chunks, each whose total
/// is `<= MAX_TIMESTAMPS_PER_RPC`. Any individual waiter whose count is
/// zero or exceeds the per-RPC cap is rejected inline with
/// `ClientError::InvalidCount` — it can never be served, and including it
/// in a chunk would either overflow `u32` accumulation or force the server
/// to reject the entire chunk.
///
/// Uses `checked_add` so a sequence of huge counts cannot wrap silently
/// into an apparently-small total.
fn chunk_queue(queue: &mut VecDeque<Waiter>) -> Vec<(u32, VecDeque<Waiter>)> {
    let mut chunks: Vec<(u32, VecDeque<Waiter>)> = Vec::new();
    let mut current: VecDeque<Waiter> = VecDeque::new();
    let mut current_total: u32 = 0;

    while let Some(w) = queue.pop_front() {
        if w.count == 0 || w.count > MAX_TIMESTAMPS_PER_RPC {
            let _ = w.respond.send(Err(ClientError::InvalidCount(w.count)));
            continue;
        }
        let fits = current_total
            .checked_add(w.count)
            .is_some_and(|sum| sum <= MAX_TIMESTAMPS_PER_RPC);
        if !fits {
            if !current.is_empty() {
                chunks.push((current_total, std::mem::take(&mut current)));
            }
            current_total = 0;
        }
        current_total += w.count;
        current.push_back(w);
    }
    if !current.is_empty() {
        chunks.push((current_total, current));
    }
    chunks
}

/// Issue one RPC per chunk, sequentially. Fail-fast: once one chunk's RPC
/// errors, subsequent chunks get the same error without burning more RPCs
/// against what is likely a failed leader or transport. Each chunk's
/// (expected_count, result, waiters) is returned for the parent task to
/// deliver.
async fn run_chunks(rpc_fn: RpcFn, chunks: Vec<(u32, VecDeque<Waiter>)>) -> Vec<ChunkResult> {
    let mut output: Vec<ChunkResult> = Vec::with_capacity(chunks.len());
    let mut failed: Option<ClientError> = None;
    for (count, waiters) in chunks {
        let result = match &failed {
            Some(e) => Err(clone_client_error(e)),
            None => {
                let result = rpc_fn(count).await;
                if let Err(ref e) = result {
                    failed = Some(clone_client_error(e));
                }
                result
            }
        };
        output.push((count, result, waiters));
    }
    output
}

fn enqueue(queue: &mut VecDeque<Waiter>, first_arrival: &mut Option<Instant>, waiter: Waiter) {
    if first_arrival.is_none() {
        *first_arrival = Some(Instant::now());
    }
    queue.push_back(waiter);
    set_queue_depth_gauge(queue.len());
}

/// Refresh the driver's waiter-queue gauge to the current size. Compiled away
/// to a no-op without the `metrics` feature so the hot path stays free of
/// branches when no recorder is installed.
#[inline]
fn set_queue_depth_gauge(depth: usize) {
    #[cfg(feature = "metrics")]
    metrics::gauge!("tsoracle.client.driver.queue_depth").set(depth as f64);
    #[cfg(not(feature = "metrics"))]
    let _ = depth;
}

/// 0 or 1: whether the driver currently has an outgoing batch in flight.
/// A scalar gauge keeps the wire shape predictable and aligns with the
/// "one batch at a time" invariant of the driver.
#[inline]
fn set_in_flight_gauge(state: u8) {
    #[cfg(feature = "metrics")]
    metrics::gauge!("tsoracle.client.driver.in_flight").set(f64::from(state));
    #[cfg(not(feature = "metrics"))]
    let _ = state;
}

/// Deliver one chunk's RPC outcome to its waiters.
///
/// `expected` is the count the driver passed to the RPC. If the server
/// responded with a different number of timestamps, every waiter in the
/// chunk receives a protocol-violation error — silently slicing a short
/// response would let waiters commit transactions with empty/non-fresh
/// timestamps.
fn deliver(
    waiters: &mut VecDeque<Waiter>,
    result: Result<Vec<Timestamp>, ClientError>,
    expected: u32,
) {
    match result {
        Ok(all) => {
            if all.len() != expected as usize {
                let msg = format!(
                    "tsoracle protocol violation: requested {} timestamps, server returned {}",
                    expected,
                    all.len(),
                );
                while let Some(w) = waiters.pop_front() {
                    let _ = w
                        .respond
                        .send(Err(ClientError::Rpc(tonic::Status::internal(msg.clone()))));
                }
                return;
            }
            let mut iter = all.into_iter();
            while let Some(w) = waiters.pop_front() {
                let slice: Vec<Timestamp> = iter.by_ref().take(w.count as usize).collect();
                debug_assert_eq!(
                    slice.len(),
                    w.count as usize,
                    "chunk_queue established total == sum(count); a short slice means \
                     either chunk_queue or the response-length check above is wrong"
                );
                let _ = w.respond.send(Ok(slice));
            }
        }
        Err(e) => {
            while let Some(w) = waiters.pop_front() {
                let _ = w.respond.send(Err(clone_client_error(&e)));
            }
        }
    }
}

/// `ClientError` contains `tonic::Status` and `tonic::transport::Error`,
/// neither of which is `Clone`. To fan one RPC error out across every
/// waiter in a failed chunk we have to reconstruct an equivalent error.
/// `Transport` collapses to `NoReachableEndpoints` because the underlying
/// transport details aren't useful for downstream callers, and we can't
/// duplicate the original error value.
fn clone_client_error(error: &ClientError) -> ClientError {
    match error {
        ClientError::Rpc(status) => {
            ClientError::Rpc(tonic::Status::new(status.code(), status.message()))
        }
        ClientError::Transport(_) => ClientError::NoReachableEndpoints,
        ClientError::NoReachableEndpoints => ClientError::NoReachableEndpoints,
        ClientError::InvalidEndpoint(endpoint) => ClientError::InvalidEndpoint(endpoint.clone()),
        ClientError::InvalidCount(count) => ClientError::InvalidCount(*count),
        ClientError::Connector(source) => ClientError::Connector(source.to_string().into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use tsoracle_core::LOGICAL_MAX;

    /// Stub RPC that records every `count` it was called with and returns
    /// exactly that many timestamps. Used to assert chunking shape.
    fn recording_ok_rpc(
        calls: Arc<Mutex<Vec<u32>>>,
    ) -> impl Fn(u32) -> futures::future::BoxFuture<'static, Result<Vec<Timestamp>, ClientError>>
    + Send
    + Sync
    + 'static {
        move |count: u32| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.lock().push(count);
                let timestamps: Vec<Timestamp> = (0..count)
                    .map(|i| Timestamp::pack(1_000, i % (LOGICAL_MAX + 1)))
                    .collect();
                Ok(timestamps)
            })
        }
    }

    /// A coalesced batch whose total exceeds the server's per-call cap
    /// (`LOGICAL_MAX + 1`) must be split into multiple RPCs, none of which
    /// individually exceed the cap. Every waiter must still receive its
    /// full requested count.
    #[tokio::test]
    async fn coalesced_batch_above_per_rpc_cap_is_chunked() {
        let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let driver = Arc::new(Driver::spawn(
            recording_ok_rpc(calls.clone()),
            Duration::from_millis(50),
        ));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let driver = driver.clone();
            handles.push(tokio::spawn(async move { driver.request(100_000).await }));
        }
        let results = futures::future::join_all(handles).await;

        for result in &results {
            let timestamps = result
                .as_ref()
                .expect("task join")
                .as_ref()
                .expect("request must succeed");
            assert_eq!(
                timestamps.len(),
                100_000,
                "each waiter must get its full count"
            );
        }

        let observed = calls.lock().clone();
        assert!(
            observed.iter().all(|&count| count <= LOGICAL_MAX + 1),
            "every RPC must respect the per-call cap; observed counts: {observed:?}",
        );
        let total: u64 = observed.iter().map(|&count| count as u64).sum();
        assert_eq!(
            total, 400_000,
            "exactly 4 * 100_000 timestamps must be issued across all chunks"
        );
        assert!(
            observed.len() >= 2,
            "a 400_000-timestamp coalesced batch must be split into >= 2 RPCs; observed: {observed:?}",
        );
    }

    /// A single waiter requesting more than the server's per-call cap can't
    /// be served by any single RPC. The driver must surface this as
    /// `InvalidCount` rather than letting the request enter the chunk
    /// machinery (where it would either get stuck or fail every sibling).
    #[tokio::test]
    async fn waiter_above_per_rpc_cap_gets_invalid_count() {
        let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let driver = Driver::spawn(recording_ok_rpc(calls), Duration::ZERO);
        let result = driver.request(LOGICAL_MAX + 2).await;
        assert!(
            matches!(result, Err(ClientError::InvalidCount(count)) if count == LOGICAL_MAX + 2),
            "expected InvalidCount({}), got {:?}",
            LOGICAL_MAX + 2,
            result
        );
    }

    /// An oversize waiter must be rejected in isolation — its presence in a
    /// coalescing window must not poison sibling waiters that individually
    /// fit. This is the property that makes the chunk machinery safe to
    /// share between well-behaved and pathological callers.
    #[tokio::test]
    async fn oversize_waiter_does_not_poison_sibling_waiters() {
        let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let driver = Arc::new(Driver::spawn(
            recording_ok_rpc(calls),
            Duration::from_millis(50),
        ));

        let small1 = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.request(5).await })
        };
        let oversize = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.request(LOGICAL_MAX + 2).await })
        };
        let small2 = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.request(7).await })
        };

        let small1_r = small1.await.unwrap();
        let oversize_r = oversize.await.unwrap();
        let small2_r = small2.await.unwrap();

        assert!(
            matches!(oversize_r, Err(ClientError::InvalidCount(_))),
            "oversize waiter must get InvalidCount, got {oversize_r:?}",
        );
        assert_eq!(
            small1_r.expect("small1 must succeed").len(),
            5,
            "sibling small waiter must still get its full count"
        );
        assert_eq!(
            small2_r.expect("small2 must succeed").len(),
            7,
            "sibling small waiter must still get its full count"
        );
    }

    /// If the server returns fewer timestamps than requested, the driver must
    /// surface an error rather than silently delivering a short slice. A
    /// short slice would let downstream callers commit transactions with a
    /// non-fresh or even empty timestamp.
    #[tokio::test]
    async fn short_response_errors_waiters_in_chunk() {
        let rpc = |count: u32| -> futures::future::BoxFuture<
            'static,
            Result<Vec<Timestamp>, ClientError>,
        > {
            Box::pin(async move {
                let short = count.saturating_sub(1);
                let timestamps: Vec<Timestamp> = (0..short)
                    .map(|i| Timestamp::pack(1_000, i % (LOGICAL_MAX + 1)))
                    .collect();
                Ok(timestamps)
            })
        };
        let driver = Driver::spawn(rpc, Duration::ZERO);
        let result = driver.request(5).await;
        assert!(
            matches!(result, Err(ClientError::Rpc(_))),
            "short response must error, got {result:?}",
        );
    }

    /// Symmetric: more timestamps than requested also indicates a protocol
    /// violation; silently dropping extras hides server bugs.
    #[tokio::test]
    async fn long_response_errors_waiters_in_chunk() {
        let rpc = |count: u32| -> futures::future::BoxFuture<
            'static,
            Result<Vec<Timestamp>, ClientError>,
        > {
            Box::pin(async move {
                let long = count.saturating_add(3);
                let timestamps: Vec<Timestamp> = (0..long)
                    .map(|i| Timestamp::pack(1_000, i % (LOGICAL_MAX + 1)))
                    .collect();
                Ok(timestamps)
            })
        };
        let driver = Driver::spawn(rpc, Duration::ZERO);
        let result = driver.request(5).await;
        assert!(
            matches!(result, Err(ClientError::Rpc(_))),
            "long response must error, got {result:?}",
        );
    }

    #[test]
    fn clone_client_error_preserves_rpc_code_and_message() {
        let original = ClientError::Rpc(tonic::Status::failed_precondition("nope"));
        let cloned = clone_client_error(&original);
        match cloned {
            ClientError::Rpc(status) => {
                assert_eq!(status.code(), tonic::Code::FailedPrecondition);
                assert_eq!(status.message(), "nope");
            }
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clone_client_error_collapses_transport_to_no_reachable_endpoints() {
        // Constructing a `tonic::transport::Error` directly isn't possible —
        // it has no public constructor. Trigger one by attempting to dial a
        // closed port; the resulting error is then handed to
        // `clone_client_error` to confirm the collapse.
        let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
        let transport_err = endpoint
            .connect()
            .await
            .expect_err("connecting to a closed port must fail");
        let original = ClientError::Transport(transport_err);
        let cloned = clone_client_error(&original);
        assert!(matches!(cloned, ClientError::NoReachableEndpoints));
    }

    #[test]
    fn clone_client_error_preserves_simple_variants() {
        let no_endpoints = clone_client_error(&ClientError::NoReachableEndpoints);
        assert!(matches!(no_endpoints, ClientError::NoReachableEndpoints));

        let invalid_endpoint =
            clone_client_error(&ClientError::InvalidEndpoint("garbage://".into()));
        match invalid_endpoint {
            ClientError::InvalidEndpoint(s) => assert_eq!(s, "garbage://"),
            other => panic!("expected InvalidEndpoint, got {other:?}"),
        }

        let invalid_count = clone_client_error(&ClientError::InvalidCount(99));
        match invalid_count {
            ClientError::InvalidCount(c) => assert_eq!(c, 99),
            other => panic!("expected InvalidCount, got {other:?}"),
        }
    }

    /// `run_chunks` fail-fast: once one chunk errors, every subsequent
    /// chunk gets a cloned copy of the same error without burning another
    /// RPC. This is the only path that flows through line 192 (the
    /// `if let Some(err) = &failed` branch).
    #[tokio::test]
    async fn run_chunks_fails_subsequent_chunks_fast() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let calls_for_rpc = rpc_calls.clone();
        let rpc = move |_count: u32| -> futures::future::BoxFuture<
            'static,
            Result<Vec<Timestamp>, ClientError>,
        > {
            calls_for_rpc.fetch_add(1, Ordering::Relaxed);
            Box::pin(async {
                Err(ClientError::Rpc(tonic::Status::unavailable(
                    "synthetic outage",
                )))
            })
        };
        let driver = Arc::new(Driver::spawn(rpc, Duration::from_millis(10)));
        // Four waiters of LOGICAL_MAX+1 each: total = 4 * (LOGICAL_MAX+1).
        // Coalescing produces a single batch, which is then split into 4
        // chunks (one per per-call cap). The first chunk's RPC errors, and
        // chunks 2–4 must receive cloned errors without further RPCs.
        let mut handles = Vec::new();
        for _ in 0..4 {
            let driver_handle = driver.clone();
            handles.push(tokio::spawn(async move {
                driver_handle.request(LOGICAL_MAX + 1).await
            }));
        }
        let results = futures::future::join_all(handles).await;
        for r in results {
            let outer = r.expect("join");
            assert!(
                matches!(outer, Err(ClientError::Rpc(_))),
                "every waiter must see an Rpc error, got {outer:?}",
            );
        }
        // Exactly one RPC: the first chunk errored and fail-fast suppressed
        // the others. (More than one would mean the fail-fast guard was
        // bypassed.)
        let rpc_count = rpc_calls.load(Ordering::Relaxed);
        assert_eq!(rpc_count, 1, "fail-fast must issue exactly one RPC");
    }

    /// `queue non-empty + flush_interval > 0` is the path where the driver
    /// task computes a deadline from `first_arrival` and waits for siblings
    /// up to that deadline before dispatching. A lone waiter that never gets
    /// siblings must still be served (no deadlock), and the dispatch must
    /// not happen before the deadline — otherwise the driver isn't actually
    /// honouring the coalescing window.
    ///
    /// Runs under a paused tokio clock so the timing assertion is
    /// deterministic instead of wall-clock-dependent.
    #[tokio::test(start_paused = true)]
    async fn lone_waiter_dispatches_after_flush_interval() {
        let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let flush = Duration::from_millis(100);
        let driver = Driver::spawn(recording_ok_rpc(calls.clone()), flush);

        let start = Instant::now();
        let timestamps = driver.request(5).await.expect("request must succeed");
        let elapsed = start.elapsed();

        assert_eq!(
            timestamps.len(),
            5,
            "lone waiter must receive its full count"
        );
        assert_eq!(
            calls.lock().clone(),
            vec![5],
            "exactly one RPC of count 5 must be issued",
        );
        assert!(
            elapsed >= flush,
            "dispatch fired at {elapsed:?}, before the {flush:?} flush deadline",
        );
    }
}
