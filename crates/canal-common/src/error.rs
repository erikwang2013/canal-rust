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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        assert_eq!(
            format!("{}", CanalError::BinlogConnection("timeout".into())),
            "binlog connection: timeout"
        );
        assert_eq!(
            format!("{}", CanalError::PositionNotFound("bin.001".into(), 42)),
            "position bin.001:42 not found"
        );
        assert_eq!(
            format!("{}", CanalError::Protocol("bad packet".into())),
            "protocol: bad packet"
        );
        assert_eq!(
            format!("{}", CanalError::Internal("oops".into())),
            "internal: oops"
        );
    }

    #[test]
    fn test_error_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let canal_err: CanalError = io_err.into();
        assert!(matches!(canal_err, CanalError::Io(_)));
    }
}
