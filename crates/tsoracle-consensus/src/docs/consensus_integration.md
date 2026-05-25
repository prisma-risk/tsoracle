# Implementing ConsensusDriver

The `ConsensusDriver` trait in `tsoracle-consensus` is the single injection point for HA and durable persistence. Three methods, about fifty lines of trait surface. Here is what each one must do.

## leadership_events

Return a `Stream<Item = LeaderState>` that emits transitions for the lifetime of the driver. The first item MUST be the current state, available synchronously on the first poll — not withheld until the next transition fires; subsequent items reflect transitions. This is the normative contract (see the rustdoc on `ConsensusDriver::leadership_events`): the server's leader-watch task blocks on that first item, so a driver that emits it lazily stalls the fence and a node already leading at subscription time never seeds Serving. Use `tokio::sync::watch` + `tokio_stream::wrappers::WatchStream` for the canonical implementation — the watch is seeded with an initial value the stream yields immediately. The server consumes one stream per driver and holds it forever.

`LeaderState::Leader { epoch }` means this node is the elected leader at the named epoch. The epoch is opaque to the library; drivers typically map it to the consensus layer's term or lease generation. `LeaderState::Follower { leader_endpoint, leader_epoch }` means this node is a follower; if `leader_endpoint` is `Some`, the value is the *advertised tsoracle service address* of the current leader (NOT its raft / consensus address); if `leader_epoch` is `Some`, it is the leader's epoch (raft term) that this follower has accepted — the server forwards it in the `LeaderHint` trailer so clients can reject a stale follower's lower-epoch redirect. `LeaderState::Unknown` means the driver does not currently know who is leader (election in progress, network partition, etc.).

## load_high_water

Return the durably-persisted high-water. The read MUST be linearized — the returned value must reflect all writes that durably committed before this call started, from any prior leader at any prior epoch. This is the contract the failover fence depends on.

- **openraft:** call `Raft::ensure_linearizable(ReadPolicy::ReadIndex)` and read the high-water field from the state machine after the barrier passes.
- **raft-rs:** issue a `ReadIndex` request, wait for the returned index to be applied, read from the state machine.
- **etcd:** read with `--consistency=l` (linearizable, the default).
- **Single-node:** read the in-memory cache or the file. No consensus means trivially linearized.

## persist_high_water

"Advance the durable high-water to at least `at_least`, return the actual value." Critical properties: monotonic-advance, durable before returning Ok, and stale-leader rejection. The `epoch` enables that last property, but the *mechanism* is the driver's choice: a driver MAY reject a stale-leader write with an up-front `epoch` pre-check (returning `ConsensusError::Fenced`), or rely on the `max(prev, at_least)` apply plus consensus leader-forwarding (returning `ConsensusError::NotLeader`) and ignore `epoch` entirely. Both are trait-compliant; the server treats `Fenced` and `NotLeader` identically — "this leader is stale, retry against the new leader." See the rustdoc on `ConsensusDriver::persist_high_water` for the normative statement.

- **openraft:** submit `TsoExtend { at_least, epoch }` through `Raft::client_write()`. State machine apply does `stored = max(stored, at_least)`; returns the post-apply value. Stale leaders' writes fail because openraft refuses non-leader client_writes.
- **raft-rs:** propose a `TsoExtend` log entry. On commit, apply does `max(stored, at_least)`. Stale leaders fail at the propose layer.
- **etcd:** transactional update: read current value, compare-and-swap with `max(current, at_least)`. The lease + revision number gives you epoch fencing.
- **Single-node:** read current value under a mutex, take max, write the record atomically (write-then-rename + dir fsync), return.

## Single-leader is intrinsic, not a consensus choice

Any correct TSO has at most one writer to the durable high-water at any moment. This is irreducible — concurrent writers can issue duplicate timestamps. So the `ConsensusDriver` contract implicitly requires single-writer-at-a-time. Multi-writer "consensus" implementations (CRDT, last-write-wins) are not compatible with tsoracle.

## A worked sketch: openraft

```rust,ignore
// Pseudocode — adapt to your openraft TypeConfig and state machine shape.

struct OpenraftDriver {
    raft:  openraft::Raft<TypeConfig>,
    state: Arc<RwLock<StateMachine>>,
    leader_events: watch::Receiver<LeaderState>,
}

#[async_trait]
impl ConsensusDriver for OpenraftDriver {
    fn leadership_events(&self) -> Pin<Box<dyn Stream<Item = LeaderState> + Send>> {
        Box::pin(WatchStream::new(self.leader_events.clone()).boxed())
    }
    async fn load_high_water(&self) -> Result<u64, ConsensusError> {
        self.raft
            .ensure_linearizable(ReadPolicy::ReadIndex)
            .await
            .map_err(|e| ConsensusError::TransientDriver(Box::new(e)))?;
        Ok(self.state.read().high_water)
    }
    async fn persist_high_water(&self, at_least: u64, epoch: Epoch) -> Result<u64, ConsensusError> {
        let req = TsoExtend { at_least, epoch: epoch.0 };
        match self.raft.client_write(req).await {
            Ok(resp) => Ok(resp.data.persisted),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => {
                Err(ConsensusError::NotLeader { observed: None })
            }
            Err(e) => Err(ConsensusError::TransientDriver(Box::new(e))),
        }
    }
}
```

The `leader_events` watch is populated by a separate task that consumes `Raft::metrics()` and maps `current_leader` + `state` into `LeaderState`. Mapping node IDs to advertised tsoracle endpoints is the driver's job; the library never sees raw node IDs.
