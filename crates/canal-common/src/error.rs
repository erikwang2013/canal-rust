use thiserror::Error;

#[derive(Error, Debug)]
pub enum CanalError {
    #[error("binlog connection: {0}")]
    BinlogConnection(String),

    #[error("position {0}:{1} not found")]
    PositionNotFound(String, u64),

    #[error("protocol: {0}")]
    Protocol(String),

    #[error("authentication failed for client {0}")]
    AuthFailed(String),

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("store: {0}")]
    Store(String),

    #[error("configuration: {0}")]
    Config(String),

    #[error("not found: {0}")]
    NotFound(String),

    #[error("internal: {0}")]
    Internal(String),
}

pub type CanalResult<T> = Result<T, CanalError>;
