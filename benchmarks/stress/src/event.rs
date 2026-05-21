//! `SupervisorEvent`: unified mpsc payload.

use crate::chaos::ChaosEvent;
use crate::sample::{IssuedSample, LivenessIncident};

#[derive(Debug)]
pub enum SupervisorEvent {
    Issued(IssuedSample),
    Chaos(ChaosEvent),
    Liveness(LivenessIncident),
    /// Sent after all producers have stopped sending, to trigger the
    /// supervisor's final-pass policy (see spec § "Shutdown").
    End,
}
