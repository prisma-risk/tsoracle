//! Process topology: spawned `tsoracle` binaries, POSIX-signal chaos,
//! `FAILPOINTS` env propagation. Unix-only.
//!
//! Architecture:
//! - Each child runs the production `tsoracle` binary bound to an
//!   OS-assigned port. The binary prints `serving on 127.0.0.1:NNNN` on
//!   stdout after binding; the harness scans for that line to learn the
//!   actual port.
//! - The harness owns a per-child reaper task (one tokio task per PID)
//!   that calls `child.wait()` and either swallows the exit
//!   (nemesis-initiated) or pushes a `LivenessIncident::UnexpectedServerExit`
//!   to the supervisor.
//! - Best-effort "current leader": round-robin over children. The harness
//!   has no protocol handle to discover the actual Raft leader; the
//!   supervisor's invariants stay correct because monotonicity is global
//!   and fence freshness is keyed on chaos windows rather than which
//!   specific PID was targeted.
//!
//! Chaos ops (`kill_leader`, `pause_leader`, `arm_failpoint`,
//! `disarm_failpoint`) land in follow-up commits; this one wires the
//! topology surface that doesn't need POSIX signals.

mod child;
mod failpoints_env;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::chaos::ChaosEvent;
use crate::event::SupervisorEvent;
use crate::topology::{ChaosController, NodeId};

pub use self::child::{ChildHandle, ChildSpec, spawn_child, spawn_into, supervise_child};
pub use self::failpoints_env::FailpointsEnv;

/// Time we wait for each child to print its `serving on …` line. The
/// production binary binds + prints synchronously before tonic accepts;
/// 5 s leaves ample slack for cold-start file driver init.
const RESOLVE_ADDR_TIMEOUT: Duration = Duration::from_secs(5);
const RESOLVE_ADDR_POLL: Duration = Duration::from_millis(50);

/// Grace window we give every reaper to observe the post-SIGKILL exit
/// during `shutdown`. Best-effort — `kill_on_drop = true` would catch any
/// stragglers anyway, but draining the reapers here means their final
/// state changes are observable to any test that calls `shutdown()` and
/// then inspects supervisor output.
const SHUTDOWN_DRAIN: Duration = Duration::from_millis(100);

pub struct ProcessTopology {
    pub controller: ProcessController,
}

pub struct ProcessController {
    nodes: Vec<Arc<ChildHandle>>,
    /// Round-robin index for "current target". The process topology has
    /// no protocol-level leader discovery; each chaos op rotates through
    /// children.
    round_robin: Mutex<usize>,
    /// Set by `set_liveness_tx` before any chaos is dispatched. Used by
    /// `kill_leader` to spawn a fresh reaper task after each respawn
    /// (wired in a follow-up commit).
    liveness_tx: Mutex<Option<mpsc::Sender<SupervisorEvent>>>,
    /// Shared FAILPOINTS map. `arm`/`disarm` (follow-up commit) update
    /// it; every (re)spawn snapshots the current serialization into the
    /// child's environment.
    #[allow(dead_code)] // wired by arm_failpoint / disarm_failpoint in a follow-up
    failpoints: Mutex<FailpointsEnv>,
    #[allow(dead_code)] // consumed by the chaos op implementations in follow-up commits
    grace: Duration,
    /// Held until shutdown so the per-node `state_dir`s under it get
    /// cleaned up when the controller is dropped.
    _tmp_root: TempDir,
}

impl ProcessTopology {
    pub async fn spawn(node_count: usize, grace: Duration) -> anyhow::Result<Self> {
        if node_count < 1 {
            anyhow::bail!("--nodes must be >= 1 for process topology");
        }
        let binary = locate_tsoracle_binary()?;
        let tmp_root = tempfile::tempdir()?;
        let mut nodes = Vec::with_capacity(node_count);
        for idx in 0..node_count {
            let data_dir = tmp_root.path().join(format!("node-{idx}"));
            std::fs::create_dir_all(&data_dir)?;
            let handle = spawn_child(ChildSpec {
                binary: binary.clone(),
                data_dir,
                failpoints_env: None,
            })
            .await?;
            let resolved = resolve_bound_addr(&handle).await?;
            // OnceLock::set returns Err if already set, which can't happen
            // here because we just spawned the handle.
            handle
                .addr
                .set(format!("http://{resolved}"))
                .map_err(|_| anyhow::anyhow!("addr already set on freshly-spawned child"))?;
            nodes.push(handle);
        }
        Ok(ProcessTopology {
            controller: ProcessController {
                nodes,
                round_robin: Mutex::new(0),
                liveness_tx: Mutex::new(None),
                failpoints: Mutex::new(FailpointsEnv::new()),
                grace,
                _tmp_root: tmp_root,
            },
        })
    }
}

/// Locate the production `tsoracle` binary. Two lookup strategies:
/// 1. `CARGO_BIN_EXE_tsoracle` — set by cargo for integration tests of
///    the `tsoracle-bin` crate. Not set for the stress crate's tests,
///    so the walk-up fallback is the primary path under `cargo test`.
/// 2. Walk parent directories from `current_exe()` looking for a sibling
///    `tsoracle` file. Works both for `cargo run -p stress` (the stress
///    bin sits next to tsoracle in `target/{debug,release}/`) and for
///    `cargo test -p stress` (the test bin sits in `target/{…}/deps/`,
///    one level deeper).
fn locate_tsoracle_binary() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_tsoracle") {
        return Ok(PathBuf::from(path));
    }
    let exe = std::env::current_exe()?;
    let mut dir = exe.parent().map(|parent| parent.to_path_buf());
    while let Some(d) = dir {
        let candidate = d.join("tsoracle");
        if candidate.is_file() {
            return Ok(candidate);
        }
        dir = d.parent().map(|parent| parent.to_path_buf());
    }
    anyhow::bail!(
        "tsoracle binary not found; build with `cargo build --bin tsoracle` \
         (release builds: add `--release`)"
    )
}

/// Poll `handle.recent_logs` until a `serving on <addr>` line shows up or
/// `RESOLVE_ADDR_TIMEOUT` elapses. The `tsoracle` binary prints this line
/// to stdout immediately after `TcpListener::bind` succeeds, so on a happy
/// startup we converge within tens of milliseconds.
async fn resolve_bound_addr(handle: &Arc<ChildHandle>) -> anyhow::Result<String> {
    let deadline = Instant::now() + RESOLVE_ADDR_TIMEOUT;
    loop {
        if let Some(addr) = scan_logs_for_addr(handle) {
            return Ok(addr);
        }
        if Instant::now() >= deadline {
            // Snapshot recent logs so the operator can see what the child
            // printed instead of the expected line.
            let tail: Vec<String> = handle.recent_logs.lock().iter().cloned().collect();
            anyhow::bail!(
                "timed out waiting for tsoracle child (pid {}) to print 'serving on …'; \
                 recent stdout/stderr:\n{}",
                handle.pid.load(Ordering::Relaxed),
                tail.join("\n"),
            );
        }
        tokio::time::sleep(RESOLVE_ADDR_POLL).await;
    }
}

fn scan_logs_for_addr(handle: &Arc<ChildHandle>) -> Option<String> {
    handle.recent_logs.lock().iter().find_map(|line| {
        line.strip_prefix("serving on ")
            .map(|rest| rest.trim().to_string())
    })
}

#[async_trait]
impl ChaosController for ProcessController {
    async fn kill_leader(&self) -> ChaosEvent {
        unimplemented!("kill_leader lands in a follow-up commit")
    }
    async fn pause_leader(&self, _dur: Duration) -> ChaosEvent {
        unimplemented!("pause_leader lands in a follow-up commit")
    }
    async fn arm_failpoint(&self, _name: &str, _action: &str) -> ChaosEvent {
        unimplemented!("arm_failpoint lands in a follow-up commit")
    }
    async fn disarm_failpoint(&self, _name: &str) -> ChaosEvent {
        unimplemented!("disarm_failpoint lands in a follow-up commit")
    }

    fn endpoints(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter_map(|node| node.addr.get().cloned())
            .collect()
    }

    fn current_leader(&self) -> Option<NodeId> {
        if self.nodes.is_empty() {
            return None;
        }
        let mut idx = self.round_robin.lock();
        let id = NodeId(*idx as u32);
        *idx = (*idx + 1) % self.nodes.len();
        Some(id)
    }

    async fn shutdown(self: Box<Self>) {
        for node in &self.nodes {
            // Suppress the reaper's UnexpectedServerExit emission for the
            // shutdown-driven kill.
            node.kill_expected.store(true, Ordering::Relaxed);
            let pid = node.pid.load(Ordering::Relaxed);
            if pid != 0 {
                // Best-effort: if the child already exited we simply move on.
                let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
            }
        }
        // Give reapers a moment to observe wait()-return and drop their
        // Child references so the temp dir cleanup that follows isn't
        // racing pending writes.
        tokio::time::sleep(SHUTDOWN_DRAIN).await;
        // _tmp_root drops here, recursively removing per-node state dirs.
    }

    fn set_liveness_tx(&self, tx: mpsc::Sender<SupervisorEvent>) {
        *self.liveness_tx.lock() = Some(tx.clone());
        for node in &self.nodes {
            tokio::spawn(supervise_child(node.clone(), tx.clone()));
        }
    }
}
