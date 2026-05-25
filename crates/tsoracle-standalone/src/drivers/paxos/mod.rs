use crate::Standalone;
use crate::config::PaxosConfig;
use crate::error::StandaloneError;

// Stub: replaced with the real bootstrap + peer transport in a later task.
pub(crate) async fn build_paxos(_cfg: PaxosConfig) -> Result<Standalone, StandaloneError> {
    Err(StandaloneError::Bootstrap(
        "paxos driver is not yet wired in this build".into(),
    ))
}
