//! Build and mutate the `FAILPOINTS=…` env-var string handed to each
//! spawned `tsoracle` child. The serialized form matches the
//! `rust-fail` crate's expected `key1=action1;key2=action2` shape.

use std::collections::BTreeMap;

/// Mutable map of failpoint name → action. Lives on `ProcessController`
/// behind a `Mutex`; `arm_failpoint` / `disarm_failpoint` update it and
/// every (re)spawn snapshots the current serialization into the child's
/// environment.
#[derive(Debug, Default, Clone)]
pub struct FailpointsEnv {
    map: BTreeMap<String, String>,
}

impl FailpointsEnv {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    pub fn arm(&mut self, name: &str, action: &str) {
        self.map.insert(name.into(), action.into());
    }

    pub fn disarm(&mut self, name: &str) {
        self.map.remove(name);
    }

    /// Serialize as `key1=action1;key2=action2`. Empty map → empty string.
    /// `BTreeMap` iteration order makes the output deterministic, which
    /// matters for snapshot-style tests.
    pub fn to_env(&self) -> String {
        self.map
            .iter()
            .map(|(name, action)| format!("{name}={action}"))
            .collect::<Vec<_>>()
            .join(";")
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_serializes_empty() {
        assert_eq!(FailpointsEnv::new().to_env(), "");
        assert!(FailpointsEnv::new().is_empty());
    }

    #[test]
    fn arm_disarm_round_trip() {
        let mut env = FailpointsEnv::new();
        env.arm("foo", "panic");
        env.arm("bar", "return(7)");
        assert_eq!(env.to_env(), "bar=return(7);foo=panic");
        env.disarm("foo");
        assert_eq!(env.to_env(), "bar=return(7)");
        env.disarm("missing");
        assert_eq!(env.to_env(), "bar=return(7)");
    }

    #[test]
    fn arm_overwrites_existing_action() {
        let mut env = FailpointsEnv::new();
        env.arm("foo", "panic");
        env.arm("foo", "return(0)");
        assert_eq!(env.to_env(), "foo=return(0)");
    }
}
