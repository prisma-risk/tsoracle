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

use anyhow::Context;
use async_trait::async_trait;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;
use parking_lot::Mutex;
use tempfile::TempDir;
use tokio::sync::mpsc;

use crate::chaos::{ChaosEvent, ChaosKind, ChaosOutcome};
use crate::event::SupervisorEvent;
use crate::topology::{ChaosController, NodeId, timed_event};

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

/// Gap between SIGKILL and respawn. Lets the per-child reaper observe
/// `child.wait()` return + clear `kill_expected` before the next spawn
/// fills the slot. 50 ms is dead-conservative — wait() typically resolves
/// within a few hundred microseconds of SIGKILL — but small enough that
/// the nemesis chaos window stays bounded.
const RESPAWN_GAP: Duration = Duration::from_millis(50);

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
    /// Shared FAILPOINTS map. `arm`/`disarm` update it; every (re)spawn
    /// snapshots the current serialization into the child's environment.
    failpoints: Mutex<FailpointsEnv>,
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
            let port = parse_port(&resolved).with_context(|| {
                format!("parse port from tsoracle's bind line: 'serving on {resolved}'")
            })?;
            // OnceLock::set returns Err if already set, which can't happen
            // here because we just spawned the handle.
            handle
                .addr
                .set(format!("http://{resolved}"))
                .map_err(|_| anyhow::anyhow!("addr already set on freshly-spawned child"))?;
            handle
                .port
                .set(port)
                .map_err(|_| anyhow::anyhow!("port already set on freshly-spawned child"))?;
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

/// Locate the production `tsoracle` binary. Three lookup strategies, in
/// order of preference:
/// 1. `TSORACLE_BIN` — explicit override. Used by `make coverage`, which
///    runs `cargo llvm-cov … --exclude tsoracle` and so doesn't build
///    the bin into the coverage target dir; the Makefile builds it
///    separately and passes the absolute path through this variable.
/// 2. `CARGO_BIN_EXE_tsoracle` — set by cargo for integration tests of
///    the `tsoracle-bin` crate. Not set for the stress crate's tests,
///    so this is consulted defensively for callers that happen to
///    arrange it.
/// 3. Walk parent directories from `current_exe()` looking for a sibling
///    `tsoracle` file. Works both for `cargo run -p stress` (the stress
///    bin sits next to tsoracle in `target/{debug,release}/`) and for
///    `cargo test -p stress` (the test bin sits in `target/{…}/deps/`,
///    one level deeper).
fn locate_tsoracle_binary() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("TSORACLE_BIN") {
        let p = PathBuf::from(path);
        if !p.is_file() {
            anyhow::bail!("TSORACLE_BIN points at non-existent path: {}", p.display());
        }
        return Ok(p);
    }
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
         or set TSORACLE_BIN to its path (release builds: add `--release`)"
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

/// Extract the port from a "host:port" string. Used to populate
/// `ChildHandle::port` after the initial bind so respawns can rebind to
/// the same port instead of churning through ephemeral assignments.
fn parse_port(addr: &str) -> anyhow::Result<u16> {
    let port_str = addr
        .rsplit_once(':')
        .map(|(_, port)| port)
        .ok_or_else(|| anyhow::anyhow!("no ':' separator in '{addr}'"))?;
    port_str
        .parse::<u16>()
        .with_context(|| format!("parse port '{port_str}' from '{addr}'"))
}

#[async_trait]
impl ChaosController for ProcessController {
    async fn kill_leader(&self) -> ChaosEvent {
        let grace = self.grace;
        // Snapshot inputs outside the closure so the move-future captures
        // owned, send-safe state only.
        let Some(target_id) = self.current_leader() else {
            return timed_event(ChaosKind::LeaderKill, grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader (empty topology)".into(),
                }
            })
            .await;
        };
        let idx = target_id.0 as usize;
        if idx >= self.nodes.len() {
            return timed_event(ChaosKind::LeaderKill, grace, || async {
                ChaosOutcome::Skipped {
                    reason: format!("round-robin idx {idx} out of range"),
                }
            })
            .await;
        }
        let handle = self.nodes[idx].clone();
        let pid = handle.pid.load(Ordering::Relaxed);
        if pid == 0 {
            return timed_event(ChaosKind::LeaderKill, grace, || async {
                ChaosOutcome::Skipped {
                    reason: format!("node {idx} has no live PID"),
                }
            })
            .await;
        }
        // Arm BEFORE the kill so the reaper's swap sees `true` regardless
        // of scheduling order between SIGKILL and wait()-return.
        handle.kill_expected.store(true, Ordering::Relaxed);
        let failpoints_env = self.failpoints.lock().to_env();
        let liveness_tx = self.liveness_tx.lock().clone();

        timed_event(ChaosKind::LeaderKill, grace, move || async move {
            if let Err(e) = kill(Pid::from_raw(pid as i32), Signal::SIGKILL) {
                // Disarm the kill_expected we set above: nothing happened.
                handle.kill_expected.store(false, Ordering::Relaxed);
                return ChaosOutcome::Failed {
                    reason: format!("kill(pid={pid}, SIGKILL): {e}"),
                };
            }
            // Let the reaper observe wait()-return and consume the
            // kill_expected flag before we fill `handle.child` again.
            tokio::time::sleep(RESPAWN_GAP).await;
            // Drop stale stdout (incl. the dead child's `serving on …`
            // line) so the post-respawn readiness scan can't match it.
            handle.recent_logs.lock().clear();

            let env = if failpoints_env.is_empty() {
                None
            } else {
                Some(failpoints_env.as_str())
            };
            if let Err(e) = spawn_into(&handle, env).await {
                return ChaosOutcome::Failed {
                    reason: format!("respawn(pid was {pid}): {e}"),
                };
            }
            // Wait for the new child to print `serving on …` so subsequent
            // load-gen requests don't race the gRPC accept loop.
            if let Err(e) = resolve_bound_addr(&handle).await {
                return ChaosOutcome::Failed {
                    reason: format!("post-respawn readiness: {e}"),
                };
            }
            // Start a reaper for the freshly-spawned child. The original
            // reaper consumed its one `wait()` and exited.
            if let Some(tx) = liveness_tx {
                tokio::spawn(supervise_child(handle.clone(), tx));
            }
            ChaosOutcome::Applied
        })
        .await
    }
    async fn pause_leader(&self, dur: Duration) -> ChaosEvent {
        let grace = self.grace;
        let Some(target_id) = self.current_leader() else {
            return timed_event(ChaosKind::LeaderPause, grace, || async {
                ChaosOutcome::Skipped {
                    reason: "no current leader (empty topology)".into(),
                }
            })
            .await;
        };
        let idx = target_id.0 as usize;
        if idx >= self.nodes.len() {
            return timed_event(ChaosKind::LeaderPause, grace, || async {
                ChaosOutcome::Skipped {
                    reason: format!("round-robin idx {idx} out of range"),
                }
            })
            .await;
        }
        let handle = self.nodes[idx].clone();
        let pid = handle.pid.load(Ordering::Relaxed);
        if pid == 0 {
            return timed_event(ChaosKind::LeaderPause, grace, || async {
                ChaosOutcome::Skipped {
                    reason: format!("node {idx} has no live PID"),
                }
            })
            .await;
        }

        timed_event(ChaosKind::LeaderPause, grace, move || async move {
            let target = Pid::from_raw(pid as i32);
            if let Err(e) = kill(target, Signal::SIGSTOP) {
                return ChaosOutcome::Failed {
                    reason: format!("SIGSTOP(pid={pid}): {e}"),
                };
            }
            tokio::time::sleep(dur).await;
            // Best-effort: even if SIGCONT errors (e.g. process died
            // between stop and resume), the chaos op as a whole is
            // recorded so the supervisor accounts for the window.
            if let Err(e) = kill(target, Signal::SIGCONT) {
                return ChaosOutcome::Failed {
                    reason: format!("SIGCONT(pid={pid}): {e}"),
                };
            }
            ChaosOutcome::Applied
        })
        .await
    }
    async fn arm_failpoint(&self, name: &str, action: &str) -> ChaosEvent {
        let grace = self.grace;
        let kind = ChaosKind::FailpointArm { name: name.into() };
        // Mutate the shared map before timing starts so the very next
        // respawn sees the new entry.
        self.failpoints.lock().arm(name, action);
        timed_event(kind, grace, || async {
            // Process topology: failpoint env vars are read by tsoracle at
            // process startup. Live children are unaffected; only future
            // respawns (e.g. those launched by a subsequent kill_leader)
            // observe this arming. See spec § "ProcessTopology::arm_failpoint".
            ChaosOutcome::Applied
        })
        .await
    }

    async fn disarm_failpoint(&self, name: &str) -> ChaosEvent {
        let grace = self.grace;
        let kind = ChaosKind::FailpointDisarm { name: name.into() };
        self.failpoints.lock().disarm(name);
        timed_event(kind, grace, || async { ChaosOutcome::Applied }).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicBool, AtomicU32};

    /// Build a ChildHandle stub with no live `Child`, used to exercise the
    /// log-scanning helpers without spawning the real binary.
    fn stub_handle(logs: Vec<&str>) -> Arc<ChildHandle> {
        let buffer: VecDeque<String> = logs.into_iter().map(String::from).collect();
        Arc::new(ChildHandle {
            child: parking_lot::Mutex::new(None),
            pid: AtomicU32::new(0),
            addr: OnceLock::new(),
            port: OnceLock::new(),
            binary: PathBuf::from("/dev/null"),
            data_dir: PathBuf::from("/tmp"),
            recent_logs: parking_lot::Mutex::new(buffer),
            kill_expected: AtomicBool::new(false),
        })
    }

    #[test]
    fn parse_port_extracts_trailing_port() {
        assert_eq!(parse_port("127.0.0.1:54321").unwrap(), 54321);
        // IPv6-style with multiple ':' uses rsplit so only the trailing
        // segment is parsed.
        assert_eq!(parse_port("[::1]:9000").unwrap(), 9000);
    }

    #[test]
    fn parse_port_rejects_missing_separator() {
        let err = parse_port("no-colon-here").unwrap_err().to_string();
        assert!(err.contains("no ':'"), "got {err}");
    }

    #[test]
    fn parse_port_rejects_non_numeric() {
        let err = parse_port("127.0.0.1:abc").unwrap_err().to_string();
        assert!(err.contains("parse port"), "got {err}");
    }

    #[test]
    fn scan_logs_for_addr_returns_none_when_no_serving_line() {
        let handle = stub_handle(vec!["startup banner", "another line"]);
        assert!(scan_logs_for_addr(&handle).is_none());
    }

    #[test]
    fn scan_logs_for_addr_extracts_first_serving_line() {
        let handle = stub_handle(vec!["banner", "serving on 127.0.0.1:5151", "later log"]);
        assert_eq!(
            scan_logs_for_addr(&handle).as_deref(),
            Some("127.0.0.1:5151")
        );
    }
}
