#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("no reachable endpoints")]
    NoReachableEndpoints,
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(String),
    #[error("invalid count: {0}")]
    InvalidCount(u32),
}
