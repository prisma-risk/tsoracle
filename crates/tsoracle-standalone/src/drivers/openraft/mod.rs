use crate::Standalone;
use crate::config::OpenraftConfig;
use crate::error::StandaloneError;

// Stub: replaced with the real bootstrap + peer transport in a later task.
pub(crate) async fn build_openraft(_cfg: OpenraftConfig) -> Result<Standalone, StandaloneError> {
    Err(StandaloneError::Bootstrap(
        "openraft driver is not yet wired in this build".into(),
    ))
}
