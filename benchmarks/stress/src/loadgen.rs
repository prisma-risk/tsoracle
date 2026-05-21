//! Pool of client tasks issuing GetTs / GetTsBatch.
//!
//! MIRRORS `bench-minimal::is_transient` with one local addition:
//! `FailedPrecondition` is *transient* here because it is the legitimate
//! failover-fence error code under chaos (see spec § "Client RPC errors").
//! The two copies are kept in sync manually.

use tonic::Code;
use tsoracle_client::ClientError;

pub fn is_transient(err: &ClientError) -> bool {
    match err {
        ClientError::Transport(_) => true,
        ClientError::Rpc(status) => matches!(
            status.code(),
            Code::Unavailable
                | Code::DeadlineExceeded
                | Code::ResourceExhausted
                | Code::FailedPrecondition
        ),
        ClientError::NoReachableEndpoints
        | ClientError::InvalidEndpoint(_)
        | ClientError::InvalidCount(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};
    use tsoracle_client::ClientError;

    #[test]
    fn transient_classifies_known_codes() {
        let status = Status::new(Code::Unavailable, "leader changed");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::DeadlineExceeded, "timeout");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::ResourceExhausted, "backpressure");
        assert!(is_transient(&ClientError::Rpc(status)));
        let status = Status::new(Code::FailedPrecondition, "fence active");
        assert!(is_transient(&ClientError::Rpc(status)));
    }

    #[test]
    fn non_transient_rejected() {
        let status = Status::new(Code::InvalidArgument, "bad batch size");
        assert!(!is_transient(&ClientError::Rpc(status)));
        assert!(!is_transient(&ClientError::NoReachableEndpoints));
    }
}
