//! Git metadata capture. MIRRORS `bench-minimal::GitInfo` — kept in sync
//! manually; see Plan A scope note for rationale.

use std::process::Command;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitInfo {
    pub rev: String,
    pub dirty: bool,
}

impl GitInfo {
    pub fn capture() -> Self {
        let rev = Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .and_then(|o| {
                if o.status.success() {
                    Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
                } else {
                    None
                }
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "unknown".to_string());

        let dirty = Command::new("git")
            .args(["diff-index", "--quiet", "HEAD", "--"])
            .status()
            .ok()
            .map(|s| s.code() == Some(1))
            .unwrap_or(false);

        GitInfo { rev, dirty }
    }
}
