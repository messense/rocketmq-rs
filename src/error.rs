use std::string::FromUtf8Error;
use std::{error, fmt, io};

#[derive(Debug)]
pub enum Error {
    Connection(ConnectionError),
    Io(io::Error),
    Json(serde_json::Error),
    InvalidUtf8(FromUtf8Error),
    InvalidHeaderCodec,
    InvalidHeader(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(err) => err.fmt(f),
            Error::Io(err) => err.fmt(f),
            Error::Json(err) => err.fmt(f),
            Error::InvalidUtf8(err) => err.fmt(f),
            Error::InvalidHeaderCodec => write!(f, "invalid header codec"),
            Error::InvalidHeader(ref err) => write!(f, "invalid header: {}", err),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Error::Connection(err) => Some(err),
            Error::Io(err) => Some(err),
            Error::Json(err) => Some(err),
            Error::InvalidUtf8(err) => Some(err),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {
    Disconnected,
    Canceled,
    Shutdown,
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionError::Disconnected => write!(f, "Disconnected"),
            ConnectionError::Canceled => write!(f, "canceled request"),
            ConnectionError::Shutdown => write!(f, "The connection was shut down"),
        }
    }
}

impl error::Error for ConnectionError {}

impl From<ConnectionError> for Error {
    fn from(err: ConnectionError) -> Self {
        Self::Connection(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(err: FromUtf8Error) -> Self {
        Self::InvalidUtf8(err)
    }
}