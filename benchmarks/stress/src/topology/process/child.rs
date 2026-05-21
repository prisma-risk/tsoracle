//! Per-child PID supervision and log capture for the process topology.
//!
//! A `ChildHandle` owns the currently-living `tokio::process::Child` plus
//! the metadata needed to respawn the same node into the same `state_dir`
//! after a SIGKILL. The handle is wrapped in `Arc` once and shared between
//! the spawn/respawn paths, the per-child reaper task (`supervise_child`),
//! and the stdio-capture tasks.
//!
//! The reaper distinguishes nemesis-initiated exits — where `kill_leader`
//! arms `kill_expected = true` immediately before sending SIGKILL — from
//! unexpected ones, emitting a `LivenessIncident::UnexpectedServerExit`
//! only in the latter case.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::Instant;

use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::mpsc;

use crate::event::SupervisorEvent;
use crate::sample::{LivenessIncident, LivenessIncidentKind};

const LOG_RING_CAPACITY: usize = 64;

/// Per-child runtime state. Shared across the spawn/respawn paths, the
/// reaper task, and the stdio-capture tasks via an outer `Arc`.
pub struct ChildHandle {
    /// The currently-living `Child`, if any. `None` between the moment the
    /// reaper takes ownership (to call `wait()`) and the moment `spawn_into`
    /// re-fills the slot during a respawn.
    pub child: Mutex<Option<Child>>,
    /// PID of the currently-living child; updated by `spawn_into` on every
    /// (re)spawn.
    pub pid: AtomicU32,
    /// Resolved bind address ("http://127.0.0.1:NNNN"). Set exactly once,
    /// during initial spawn, by `ProcessTopology::spawn`.
    pub addr: OnceLock<String>,
    /// Resolved bind port. Set exactly once during initial spawn; pinned
    /// so respawns rebind to the same port (and the harness's endpoint
    /// list — handed to clients before the first chaos op — stays valid
    /// across SIGKILL + respawn cycles).
    pub port: OnceLock<u16>,
    /// Path to the `tsoracle` binary. Stable across respawns.
    pub binary: PathBuf,
    /// Per-node state directory. Reused across respawns so the file driver's
    /// high-water survives a SIGKILL.
    pub data_dir: PathBuf,
    /// Most recent stdout+stderr lines, capped at `LOG_RING_CAPACITY`. Used
    /// to attach diagnostic context when `UnexpectedServerExit` fires.
    pub recent_logs: Mutex<VecDeque<String>>,
    /// Armed by `kill_leader` immediately before SIGKILL; the reaper swaps
    /// it back to `false` on wait()-return and uses the observed value to
    /// decide whether to emit `UnexpectedServerExit`. Also set by
    /// `shutdown` to suppress emission during teardown.
    pub kill_expected: AtomicBool,
}

impl ChildHandle {
    fn empty(binary: PathBuf, data_dir: PathBuf) -> Self {
        Self {
            child: Mutex::new(None),
            pid: AtomicU32::new(0),
            addr: OnceLock::new(),
            port: OnceLock::new(),
            binary,
            data_dir,
            recent_logs: Mutex::new(VecDeque::with_capacity(LOG_RING_CAPACITY)),
            kill_expected: AtomicBool::new(false),
        }
    }
}

/// Inputs for the first spawn of a node. Respawns go through `spawn_into`
/// directly with the existing `Arc<ChildHandle>` (so `binary` and
/// `data_dir` stay stable).
pub struct ChildSpec {
    pub binary: PathBuf,
    pub data_dir: PathBuf,
    /// Serialized `FAILPOINTS=…` value (without the leading `FAILPOINTS=`).
    /// `None` skips setting the env var.
    pub failpoints_env: Option<String>,
}

/// Spawn a fresh child for a brand-new node. Returns an `Arc<ChildHandle>`
/// owning the `Child` plus the metadata needed for later respawns.
pub async fn spawn_child(spec: ChildSpec) -> anyhow::Result<Arc<ChildHandle>> {
    let handle = Arc::new(ChildHandle::empty(spec.binary, spec.data_dir));
    spawn_into(&handle, spec.failpoints_env.as_deref()).await?;
    Ok(handle)
}

/// (Re)spawn a child into an existing `ChildHandle`. Used for both the
/// initial spawn (`spawn_child` calls this) and for respawn-after-kill
/// (`ProcessController::kill_leader` calls this with the current
/// `FAILPOINTS=…` value).
pub async fn spawn_into(
    handle: &Arc<ChildHandle>,
    failpoints_env: Option<&str>,
) -> anyhow::Result<()> {
    let listen = match handle.port.get() {
        Some(port) => format!("127.0.0.1:{port}"),
        None => "127.0.0.1:0".to_string(),
    };
    let mut cmd = Command::new(&handle.binary);
    cmd.arg("serve")
        .arg("--listen")
        .arg(&listen)
        .arg("--state-dir")
        .arg(&handle.data_dir)
        .arg("--log")
        .arg("warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if let Some(fp) = failpoints_env {
        cmd.env("FAILPOINTS", fp);
    }
    let mut child = cmd.spawn()?;
    let pid = child
        .id()
        .ok_or_else(|| anyhow::anyhow!("spawned tsoracle child has no pid"))?;
    handle.pid.store(pid, Ordering::Relaxed);

    if let Some(stdout) = child.stdout.take() {
        let reader_handle = handle.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                push_log_line(&reader_handle, line);
            }
        });
    }
    if let Some(stderr) = child.stderr.take() {
        let reader_handle = handle.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                push_log_line(&reader_handle, line);
            }
        });
    }
    *handle.child.lock() = Some(child);
    Ok(())
}

fn push_log_line(handle: &Arc<ChildHandle>, line: String) {
    let mut ring = handle.recent_logs.lock();
    if ring.len() == LOG_RING_CAPACITY {
        ring.pop_front();
    }
    ring.push_back(line);
}

/// Wait for the current child to exit, then either swallow the exit (if
/// `kill_expected` was armed) or push a
/// `LivenessIncident::UnexpectedServerExit` to the supervisor. One-shot:
/// `kill_leader` spawns a fresh task per respawn.
pub async fn supervise_child(handle: Arc<ChildHandle>, liveness_tx: mpsc::Sender<SupervisorEvent>) {
    let child_opt = handle.child.lock().take();
    let Some(mut child) = child_opt else {
        return;
    };
    let _ = child.wait().await;
    let was_expected = handle.kill_expected.swap(false, Ordering::Relaxed);
    if was_expected {
        return;
    }
    let last_log_lines: Vec<String> = handle.recent_logs.lock().iter().cloned().collect();
    let incident = LivenessIncident {
        kind: LivenessIncidentKind::UnexpectedServerExit {
            pid: handle.pid.load(Ordering::Relaxed),
            last_log_lines,
        },
        at: Instant::now(),
    };
    let _ = liveness_tx.send(SupervisorEvent::Liveness(incident)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_log_line_caps_at_capacity() {
        let handle = Arc::new(ChildHandle::empty(
            PathBuf::from("/nonexistent"),
            PathBuf::from("/nonexistent"),
        ));
        for i in 0..(LOG_RING_CAPACITY + 5) {
            push_log_line(&handle, format!("line {i}"));
        }
        let ring = handle.recent_logs.lock();
        assert_eq!(ring.len(), LOG_RING_CAPACITY);
        // First line should be "line 5" (0-4 were dropped).
        assert_eq!(ring.front().map(String::as_str), Some("line 5"));
        assert_eq!(
            ring.back().map(String::as_str),
            Some(format!("line {}", LOG_RING_CAPACITY + 4).as_str()),
        );
    }
}
