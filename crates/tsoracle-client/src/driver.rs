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
use crate::response::TimestampRange;

/// Bound on the total number of waiters that can exist inside the driver
/// at any moment. A slow server combined with a fast caller must not grow
/// this without limit; once the bound is reached, `Driver::request` awaits
/// via `Sender::send().await`, propagating backpressure to callers.
///
/// The bound is split evenly between two structures that hold waiters:
/// the `mpsc::Receiver` buffer (sized at `QUEUE_CAPACITY / 2`) and the
/// secondary `VecDeque<Waiter>` owned by `driver_task` (gated at
/// `QUEUE_CAPACITY / 2` in every `select!` arm that pulls from `rx`). The
/// sum equals `QUEUE_CAPACITY`, so the documented bound is what callers
/// actually see — the gate disables the receive arm once the secondary
/// queue is at half-capacity, the mpsc buffer fills, and the next
/// `Sender::send().await` blocks.
///
/// Each `Waiter` owns a `oneshot::Sender` whose paired receiver and
/// shared state are heap-allocated; the practical per-waiter footprint
/// is ~256 bytes, so 4096 caps total waiter memory at ~1 MB regardless
/// of how aggressive the producers are.
const QUEUE_CAPACITY: usize = 4096;

pub(crate) struct Waiter {
    pub count: u32,
    pub respond: oneshot::Sender<Result<Vec<Timestamp>, ClientError>>,
}

pub(crate) struct Driver {
    tx: mpsc::Sender<Waiter>,
}

type RpcFn = Arc<
    dyn Fn(u32) -> futures::future::BoxFuture<'static, Result<TimestampRange, ClientError>>
        + Send
        + Sync,
>;

/// `driver_task` spawns one batch task per coalesced window; the batch
/// task runs each chunk's RPC and delivers that chunk's outcome to its
/// waiters before moving to the next chunk. The handle's `()` payload
/// signals batch completion only — the per-chunk results are streamed to
/// waiters inside the task, never returned through this join.
type BatchHandle = tokio::task::JoinHandle<()>;

impl Driver {
    pub fn spawn<F>(rpc: F, flush_interval: Duration) -> Self
    where
        F: Fn(u32) -> futures::future::BoxFuture<'static, Result<TimestampRange, ClientError>>
            + Send
            + Sync
            + 'static,
    {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY / 2);
        // Spawn the driver, then spawn a one-shot observer for its
        // `JoinHandle`. The observer lives in `crate::driver_supervisor`
        // (a non-critical-path file) because it emits an info-or-higher
        // death-rattle log on panic; that log severity is banned from
        // this file by the `#[PerformanceCriticalPath]` rules.
        let handle = tokio::spawn(driver_task(Arc::new(rpc), rx, flush_interval));
        tokio::spawn(crate::driver_supervisor::observe_driver_handle(handle));
        Driver { tx }
    }

    pub async fn request(&self, count: u32) -> Result<Vec<Timestamp>, ClientError> {
        let (resp_tx, resp_rx) = oneshot::channel();
        // SendError here means the driver task's `Receiver` is gone —
        // either the driver panicked (the observer in
        // `crate::driver_supervisor` logs that) or it observed its last
        // `Sender` drop. Surface that as `DriverGone` so callers can
        // tell "local driver is dead" from
        // "network/endpoints unreachable".
        self.tx
            .send(Waiter {
                count,
                respond: resp_tx,
            })
            .await
            .map_err(|_| ClientError::DriverGone)?;
        // RecvError here means our `respond` `Sender` was dropped without
        // sending — only possible if the driver task or its chunk subtask
        // panicked while this waiter was queued. Same reasoning as above.
        resp_rx.await.map_err(|_| ClientError::DriverGone)?
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
                    // `run_chunks` already delivered each chunk's response to
                    // its waiters as that chunk's RPC completed; nothing to
                    // do here beyond clearing the in-flight slot.
                    in_flight = None;
                    set_in_flight_gauge(0);
                    if let Err(_join_err) = completed {
                        // The batch task panicked or was cancelled. Its
                        // `Waiter::respond` `Sender`s drop during unwind,
                        // so every blocked caller surfaces `DriverGone`
                        // via the `resp_rx.await.map_err` path in
                        // `Driver::request`. tokio's default panic hook
                        // also writes the panic message to stderr, so
                        // the cause isn't fully lost; this debug-level
                        // breadcrumb adds the structured context for
                        // operators who turn the level up. `debug!` is
                        // the highest severity allowed on the hot path
                        // by the performance-critical-path rules.
                        #[cfg(feature = "tracing")]
                        tracing::debug!(
                            error = %_join_err,
                            "tsoracle client batch task failed; in-flight waiters were notified via DriverGone"
                        );
                    }
                }
                next = rx.recv(), if queue.len() < QUEUE_CAPACITY / 2 => {
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
                        next = rx.recv(), if queue.len() < QUEUE_CAPACITY / 2 => {
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
            let count = w.count;
            reply_to_waiter(w, Err(ClientError::InvalidCount(count)));
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

/// Issue one RPC per chunk, sequentially, and deliver each chunk's
/// outcome to its waiters before moving on to the next chunk.
///
/// Fail-fast: once one chunk's RPC errors, subsequent chunks get the same
/// error without burning more RPCs against what is likely a failed leader
/// or transport.
///
/// Per-chunk delivery is the property that bounds retained response
/// memory at one chunk's worth, even when a slow first chunk holds the
/// batch open. The only state that crosses chunk boundaries is the
/// `failed: Option<ClientError>` used for fail-fast.
async fn run_chunks(rpc_fn: RpcFn, chunks: Vec<(u32, VecDeque<Waiter>)>) {
    let mut failed: Option<ClientError> = None;
    for (count, mut waiters) in chunks {
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
        deliver(&mut waiters, result, count);
    }
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

/// Hand one waiter its outcome over its `oneshot` channel.
///
/// `oneshot::Sender::send` fails only when the paired receiver has been
/// dropped, which here means exactly one thing: the caller dropped its
/// `Driver::request` future — a client-side timeout or cancellation — before
/// the result arrived. There is nothing left to deliver, so dropping the
/// outcome is correct; but the drop is counted via [`record_abandoned_waiter`]
/// so a cancellation storm is observable instead of silently swallowed.
/// Routing every delivery through this one helper also makes it structurally
/// impossible to add a new send site that forgets the metric.
#[inline]
fn reply_to_waiter(waiter: Waiter, outcome: Result<Vec<Timestamp>, ClientError>) {
    if waiter.respond.send(outcome).is_err() {
        record_abandoned_waiter();
    }
}

/// Count one waiter whose result could not be delivered because the caller
/// already dropped its `Driver::request` future. Compiled away to a no-op
/// without the `metrics` feature, matching the gauges above, so the hot path
/// carries no recorder cost when no metrics feature is installed.
#[inline]
fn record_abandoned_waiter() {
    #[cfg(feature = "metrics")]
    metrics::counter!("tsoracle.client.driver.abandoned_waiters.total").increment(1);
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
    result: Result<TimestampRange, ClientError>,
    expected: u32,
) {
    match result {
        Ok(range) => {
            if range.count() != expected {
                let msg = format!(
                    "tsoracle protocol violation: requested {} timestamps, server returned {}",
                    expected,
                    range.count(),
                );
                while let Some(w) = waiters.pop_front() {
                    reply_to_waiter(
                        w,
                        Err(ClientError::Rpc(tonic::Status::internal(msg.clone()))),
                    );
                }
                return;
            }
            let mut iter = range.iter();
            while let Some(w) = waiters.pop_front() {
                let slice: Vec<Timestamp> = iter.by_ref().take(w.count as usize).collect();
                debug_assert_eq!(
                    slice.len(),
                    w.count as usize,
                    "chunk_queue established total == sum(count); a short slice means \
                     either chunk_queue or the response-length check above is wrong"
                );
                reply_to_waiter(w, Ok(slice));
            }
        }
        Err(e) => {
            while let Some(w) = waiters.pop_front() {
                reply_to_waiter(w, Err(clone_client_error(&e)));
            }
        }
    }
}

/// `ClientError` contains `tonic::Status` and `tonic::transport::Error`,
/// neither of which is `Clone`. To fan one RPC error out across every
/// waiter in a failed chunk we have to reconstruct an equivalent error.
/// `Rpc` rebuilds the `Status` from its code + message and copies the
/// trailers, so the leader-hint trailer (`tsoracle-leader-hint-bin`) and
/// any other metadata survive for a sibling waiter's tracing/alerting —
/// `Status::new` alone would start with an empty metadata map.
/// `Transport` maps to [`ClientError::TransportFanout`], carrying the
/// original error's `Display` text: the value itself cannot be duplicated,
/// but the failure is still a single-endpoint transport failure and must
/// stay distinct from `NoReachableEndpoints` (whole cluster unreachable),
/// which a coalesced waiter's tracing/alerting would otherwise misread.
fn clone_client_error(error: &ClientError) -> ClientError {
    match error {
        ClientError::Rpc(status) => {
            let mut cloned = tonic::Status::new(status.code(), status.message());
            *cloned.metadata_mut() = status.metadata().clone();
            ClientError::Rpc(cloned)
        }
        ClientError::Transport(transport_error) => {
            ClientError::TransportFanout(transport_error.to_string())
        }
        ClientError::TransportFanout(message) => ClientError::TransportFanout(message.clone()),
        ClientError::NoReachableEndpoints => ClientError::NoReachableEndpoints,
        ClientError::InvalidEndpoint(endpoint) => ClientError::InvalidEndpoint(endpoint.clone()),
        ClientError::InvalidCount(count) => ClientError::InvalidCount(*count),
        ClientError::Connector(source) => ClientError::Connector(source.to_string().into()),
        ClientError::DriverGone => ClientError::DriverGone,
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
    ) -> impl Fn(u32) -> futures::future::BoxFuture<'static, Result<TimestampRange, ClientError>>
    + Send
    + Sync
    + 'static {
        move |count: u32| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.lock().push(count);
                Ok(TimestampRange::new(1_000, 0, count))
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
            Result<TimestampRange, ClientError>,
        > {
            Box::pin(async move {
                let short = count.saturating_sub(1);
                Ok(TimestampRange::new(1_000, 0, short))
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
            Result<TimestampRange, ClientError>,
        > {
            Box::pin(async move {
                let long = count.saturating_add(3);
                Ok(TimestampRange::new(1_000, 0, long))
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

    #[test]
    fn clone_client_error_preserves_rpc_metadata_trailers() {
        // A failed chunk fans its error out to every sibling waiter via
        // `clone_client_error`. The reconstructed `Rpc` status must carry the
        // original trailers — including the `tsoracle-leader-hint-bin` trailer
        // an operator would inspect on a sibling — not just code + message.
        use tonic::metadata::{BinaryMetadataKey, MetadataValue};
        use tsoracle_proto::v1::LEADER_HINT_TRAILER_KEY;

        let hint_key = BinaryMetadataKey::from_bytes(LEADER_HINT_TRAILER_KEY.as_bytes())
            .expect("LEADER_HINT_TRAILER_KEY must be a valid binary metadata key");
        let hint_payload: &[u8] = &[1, 2, 3, 4];

        let mut status = tonic::Status::failed_precondition("not leader");
        status
            .metadata_mut()
            .insert_bin(hint_key.clone(), MetadataValue::from_bytes(hint_payload));
        status
            .metadata_mut()
            .insert("x-trace-id", "abc-123".parse().expect("valid ascii value"));

        match clone_client_error(&ClientError::Rpc(status)) {
            ClientError::Rpc(cloned) => {
                assert_eq!(cloned.code(), tonic::Code::FailedPrecondition);
                assert_eq!(cloned.message(), "not leader");
                let trailer = cloned
                    .metadata()
                    .get_bin(hint_key)
                    .expect("leader-hint trailer must survive the clone");
                assert_eq!(
                    trailer.to_bytes().expect("base64-decodable").as_ref(),
                    hint_payload,
                );
                assert_eq!(
                    cloned
                        .metadata()
                        .get("x-trace-id")
                        .expect("ascii header must survive the clone"),
                    "abc-123",
                );
            }
            other => panic!("expected Rpc, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn clone_client_error_maps_transport_to_transport_fanout() {
        // Constructing a `tonic::transport::Error` directly isn't possible —
        // it has no public constructor. Trigger one by attempting to dial a
        // closed port; the resulting error is then handed to
        // `clone_client_error`. The fanned-out copy must be a
        // `TransportFanout` carrying the original message — *not*
        // `NoReachableEndpoints`, which carries the distinct meaning "the
        // whole cluster is unreachable" and would mislead tracing/alerting
        // on every sibling waiter in the coalesced chunk.
        let endpoint = tonic::transport::Endpoint::from_static("http://127.0.0.1:1");
        let transport_err = endpoint
            .connect()
            .await
            .expect_err("connecting to a closed port must fail");
        let expected_message = transport_err.to_string();
        let original = ClientError::Transport(transport_err);
        match clone_client_error(&original) {
            ClientError::TransportFanout(message) => {
                assert_eq!(message, expected_message);
            }
            other => panic!("expected TransportFanout, got {other:?}"),
        }
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

        let driver_gone = clone_client_error(&ClientError::DriverGone);
        assert!(matches!(driver_gone, ClientError::DriverGone));
    }

    #[test]
    fn clone_client_error_reclones_transport_fanout_preserving_message() {
        // `run_chunks` fail-fast stores the *cloned* error and re-clones it
        // for every subsequent chunk, so `clone_client_error` is called on a
        // value it already produced. Re-cloning a `TransportFanout` must keep
        // it a `TransportFanout` with the same message — never silently decay
        // into `NoReachableEndpoints` on the second and later chunks.
        let original = ClientError::TransportFanout("dial 10.0.0.7:50051 refused".into());
        match clone_client_error(&original) {
            ClientError::TransportFanout(message) => {
                assert_eq!(message, "dial 10.0.0.7:50051 refused");
            }
            other => panic!("expected TransportFanout, got {other:?}"),
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
            Result<TimestampRange, ClientError>,
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

    /// With one in-flight RPC stalled, the driver must not let its
    /// secondary queue grow without bound: producers exceeding the
    /// documented `QUEUE_CAPACITY` waiter bound must backpressure on
    /// `Sender::send().await` until existing waiters are delivered.
    ///
    /// White-box observation: if the secondary `VecDeque<Waiter>` were
    /// unbounded, every waiter arriving while the first RPC stalled would
    /// pile into a single follow-up batch, and after the stall releases
    /// the largest `count` argument seen by the rpc closure would equal
    /// the size of that pile. With real backpressure, every dispatched
    /// batch is bounded by `QUEUE_CAPACITY`.
    #[tokio::test]
    async fn driver_bounds_in_flight_batch_to_queue_capacity() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::watch;

        let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
        let rpc_invocations = Arc::new(AtomicUsize::new(0));
        let (released_tx, released_rx) = watch::channel(false);

        let calls_for_rpc = calls.clone();
        let rpc_invocations_for_rpc = rpc_invocations.clone();
        let released_rx_for_rpc = released_rx.clone();
        let rpc = move |count: u32| -> futures::future::BoxFuture<
            'static,
            Result<TimestampRange, ClientError>,
        > {
            let is_first = rpc_invocations_for_rpc.fetch_add(1, Ordering::SeqCst) == 0;
            let mut released = released_rx_for_rpc.clone();
            let calls = calls_for_rpc.clone();
            Box::pin(async move {
                if is_first {
                    while !*released.borrow() {
                        released
                            .changed()
                            .await
                            .expect("watch sender must outlive the rpc");
                    }
                }
                calls.lock().push(count);
                Ok(TimestampRange::new(1_000, 0, count))
            })
        };

        let driver = Arc::new(Driver::spawn(rpc, Duration::ZERO));

        // Fire 2 * QUEUE_CAPACITY concurrent requests, comfortably above the
        // documented bound so that a missing gate produces a follow-up batch
        // of `count > QUEUE_CAPACITY`.
        let total = 2 * QUEUE_CAPACITY;
        let mut handles = Vec::with_capacity(total);
        for _ in 0..total {
            let driver = driver.clone();
            handles.push(tokio::spawn(async move { driver.request(1).await }));
        }

        // Yield enough times for the driver to absorb everything it can into
        // its internal buffers. Anything past the bound must stay blocked on
        // `Sender::send().await`.
        for _ in 0..2_000 {
            tokio::task::yield_now().await;
        }

        released_tx.send(true).expect("release the stalled rpc");

        let results = futures::future::join_all(handles).await;
        for result in results {
            let outer = result.expect("join must succeed");
            let timestamps = outer.expect("request must succeed");
            assert_eq!(timestamps.len(), 1);
        }

        let observed = calls.lock().clone();
        let max_count = *observed.iter().max().expect("at least one rpc was issued");
        let batches = observed.len();
        assert!(
            max_count <= QUEUE_CAPACITY as u32,
            "max batch count ({max_count}) exceeded documented bound ({QUEUE_CAPACITY}); fired {total} requests, observed {batches} batches"
        );
    }

    /// A slow later chunk's RPC must not delay delivery of an earlier
    /// chunk's response. Each chunk's outcome must stream out as that
    /// chunk's RPC completes, not be held until every sibling chunk in
    /// the batch finishes — otherwise an early-arrival waiter pays the
    /// latency tail of the slowest sibling.
    #[tokio::test]
    async fn first_chunk_delivers_before_slow_second_chunk_completes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Notify;

        let rpc_invocations = Arc::new(AtomicUsize::new(0));
        let release_second = Arc::new(Notify::new());

        let rpc_invocations_for_rpc = rpc_invocations.clone();
        let release_second_for_rpc = release_second.clone();
        let rpc = move |count: u32| -> futures::future::BoxFuture<
            'static,
            Result<TimestampRange, ClientError>,
        > {
            let invocation = rpc_invocations_for_rpc.fetch_add(1, Ordering::SeqCst);
            let release = release_second_for_rpc.clone();
            Box::pin(async move {
                // The first invocation returns instantly; every later
                // invocation waits for the test to notify.
                if invocation >= 1 {
                    release.notified().await;
                }
                Ok(TimestampRange::new(1_000, 0, count))
            })
        };

        let driver = Arc::new(Driver::spawn(rpc, Duration::from_millis(50)));

        // Two waiters each at the per-RPC cap force `chunk_queue` to emit
        // two single-waiter chunks.
        let first = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.request(LOGICAL_MAX + 1).await })
        };
        let second = {
            let driver = driver.clone();
            tokio::spawn(async move { driver.request(LOGICAL_MAX + 1).await })
        };

        // The first chunk's rpc completes immediately. `first` must receive
        // its response without waiting for the stalled second chunk; if the
        // driver accumulates chunk results before delivering, this await
        // never completes within the timeout.
        let first_timestamps = tokio::time::timeout(Duration::from_secs(2), first)
            .await
            .expect("first chunk must deliver before second chunk's rpc completes")
            .expect("join")
            .expect("first request must succeed");
        assert_eq!(first_timestamps.len(), (LOGICAL_MAX + 1) as usize);

        assert!(
            !second.is_finished(),
            "second waiter must still be pending while its chunk's rpc is stalled",
        );

        release_second.notify_waiters();
        let second_timestamps = second
            .await
            .expect("join")
            .expect("second request must succeed");
        assert_eq!(second_timestamps.len(), (LOGICAL_MAX + 1) as usize);
    }

    /// A panic in the user-supplied RPC closure must surface to the blocked
    /// waiter as `ClientError::DriverGone` — *not* `NoReachableEndpoints`,
    /// since the network is fine and the bug is local. This is the
    /// distinction that lets an operator skip a fruitless network
    /// investigation and look at their `tracing` logs for the panic.
    #[tokio::test]
    async fn rpc_closure_panic_surfaces_as_driver_gone() {
        let rpc = |_count: u32| -> futures::future::BoxFuture<
            'static,
            Result<TimestampRange, ClientError>,
        > {
            Box::pin(async {
                panic!("synthetic panic exercising driver supervision");
            })
        };
        let driver = Driver::spawn(rpc, Duration::ZERO);
        let result = driver.request(5).await;
        assert!(
            matches!(result, Err(ClientError::DriverGone)),
            "panicking RPC closure must surface as DriverGone, got {result:?}",
        );
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

    /// Property tests: across thousands of randomly-generated arrival
    /// schedules with mixed waiter counts, the driver must always
    /// (a) deliver each waiter exactly its requested number of timestamps,
    /// (b) respect the per-RPC cap so chunk_queue never violates it, and
    /// (c) match the documented total-timestamp accounting (sum returned
    ///     == sum requested).
    ///
    /// These invariants are independent of the specific bound the issue #74
    /// fix tightens; they guard the freshness/chunking math against
    /// regressions a fixed-input unit test would miss.
    mod proptest_invariants {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #![proptest_config(ProptestConfig {
                // Each case spins up a small runtime and spawns up to ~150
                // tasks; 64 cases is enough to exercise interesting
                // schedules without making the suite slow.
                cases: 64,
                .. ProptestConfig::default()
            })]

            #[test]
            fn random_schedules_serve_every_waiter_correctly(
                counts in prop::collection::vec(1u32..=MAX_TIMESTAMPS_PER_RPC, 1..150),
                flush_micros in 0u64..2_000,
            ) {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio runtime must build");
                runtime.block_on(async {
                    let calls: Arc<Mutex<Vec<u32>>> = Arc::new(Mutex::new(Vec::new()));
                    let driver = Arc::new(Driver::spawn(
                        recording_ok_rpc(calls.clone()),
                        Duration::from_micros(flush_micros),
                    ));

                    let mut handles = Vec::with_capacity(counts.len());
                    for count in counts.iter().copied() {
                        let driver = driver.clone();
                        handles.push(tokio::spawn(async move {
                            driver.request(count).await
                        }));
                    }
                    let results = futures::future::join_all(handles).await;

                    // Every waiter must succeed with exactly its requested
                    // number of timestamps.
                    let mut total_served: u64 = 0;
                    for (idx, (requested, result)) in counts
                        .iter()
                        .zip(results.into_iter())
                        .enumerate()
                    {
                        let timestamps = result
                            .expect("join must succeed")
                            .expect("request must succeed");
                        let served = timestamps.len();
                        assert_eq!(served, *requested as usize, "request {idx} requested {requested}, served {served}");
                        total_served += served as u64;
                    }

                    let observed = calls.lock().clone();
                    // No RPC may exceed the per-call cap. chunk_queue
                    // splits batches precisely to honour this; any
                    // violation here would point at a refactor that
                    // accidentally produced an over-sized chunk.
                    for rpc_count in &observed {
                        assert!(*rpc_count <= MAX_TIMESTAMPS_PER_RPC, "rpc dispatched with count {rpc_count} > per-call cap {MAX_TIMESTAMPS_PER_RPC}; observed: {observed:?}");
                        assert!(*rpc_count > 0, "rpc dispatched with count 0");
                    }

                    // Accounting closure: the driver must never lose or
                    // duplicate timestamps relative to the schedule.
                    let total_requested: u64 = counts.iter().map(|c| u64::from(*c)).sum();
                    let total_rpc: u64 = observed.iter().map(|c| u64::from(*c)).sum();
                    assert_eq!(total_served, total_requested, "served {total_served} timestamps, requested {total_requested}");
                    assert_eq!(total_rpc, total_requested, "rpc-side total {total_rpc} != requested {total_requested}");
                });
            }
        }
    }

    /// A failed `respond.send` — the caller dropped its `Driver::request`
    /// future before the result arrived — must bump
    /// `tsoracle.client.driver.abandoned_waiters.total`, so a cancellation
    /// storm is observable rather than silently swallowed. A live receiver
    /// must leave the counter untouched.
    ///
    /// The helper is driven directly on the test thread (not through a spawned
    /// `driver_task`) because `metrics::with_local_recorder` installs a
    /// *thread-local* recorder; an emission from another worker thread would
    /// not be captured.
    #[cfg(feature = "metrics")]
    mod abandoned_waiter_metric {
        use super::*;
        use metrics_util::{
            MetricKind,
            debugging::{DebugValue, DebuggingRecorder},
        };

        type RecordedMetric = (
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        );

        const METRIC: &str = "tsoracle.client.driver.abandoned_waiters.total";

        fn counter(snapshot: &[RecordedMetric], name: &str) -> u64 {
            for (composite, _unit, _desc, value) in snapshot {
                if composite.kind() == MetricKind::Counter && composite.key().name() == name {
                    if let DebugValue::Counter(n) = value {
                        return *n;
                    }
                }
            }
            0
        }

        #[test]
        fn dropped_receiver_increments_counter() {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                let (respond, rx) = oneshot::channel();
                // The caller gave up: its receiver is gone before delivery.
                drop(rx);
                reply_to_waiter(Waiter { count: 1, respond }, Ok(Vec::new()));
            });
            assert_eq!(
                counter(&snapshotter.snapshot().into_vec(), METRIC),
                1,
                "a send to a dropped receiver must count exactly one abandoned waiter",
            );
        }

        #[test]
        fn live_receiver_does_not_increment_counter() {
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();
            metrics::with_local_recorder(&recorder, || {
                // The receiver stays alive for the whole delivery, so the send
                // succeeds and nothing is abandoned.
                let (respond, _rx) = oneshot::channel();
                reply_to_waiter(Waiter { count: 1, respond }, Ok(Vec::new()));
            });
            assert_eq!(
                counter(&snapshotter.snapshot().into_vec(), METRIC),
                0,
                "a delivery to a live receiver must not count an abandoned waiter",
            );
        }
    }
}
