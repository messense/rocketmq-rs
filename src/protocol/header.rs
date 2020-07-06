use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use num_enum::TryFromPrimitive;
use serde::{Deserialize, Serialize};

use crate::Error;

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, TryFromPrimitive)]
pub enum LanguageCode {
    JAVA = 0,
    CPP = 1,
    DOTNET = 2,
    PYTHON = 3,
    DELPHI = 4,
    ERLANG = 5,
    RUBY = 6,
    OTHER = 7,
    HTTP = 8,
    GO = 9,
    PHP = 10,
    OMS = 11,
}

impl FromStr for LanguageCode {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        let code = match s {
            "JAVA" => Self::JAVA,
            "CPP" => Self::CPP,
            "DOTNET" => Self::DOTNET,
            "PYTHON" => Self::PYTHON,
            "DELPHI" => Self::DELPHI,
            "ERLANG" => Self::ERLANG,
            "RUBY" => Self::RUBY,
            "HTTP" => Self::HTTP,
            "GO" => Self::GO,
            "PHP" => Self::PHP,
            "OMS" => Self::OMS,
            _ => Self::OTHER,
        };
        Ok(code)
    }
}

impl fmt::Display for LanguageCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            LanguageCode::JAVA => write!(f, "JAVA"),
            LanguageCode::CPP => write!(f, "CPP"),
            LanguageCode::DOTNET => write!(f, "DOTNET"),
            LanguageCode::PYTHON => write!(f, "PYTHON"),
            LanguageCode::DELPHI => write!(f, "DELPHI"),
            LanguageCode::ERLANG => write!(f, "ERLANG"),
            LanguageCode::RUBY => write!(f, "RUBY"),
            LanguageCode::OTHER => write!(f, "OTHER"),
            LanguageCode::HTTP => write!(f, "HTTP"),
            LanguageCode::GO => write!(f, "GO"),
            LanguageCode::PHP => write!(f, "PHP"),
            LanguageCode::OMS => write!(f, "OMS"),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Header {
    pub code: isize,
    pub language: LanguageCode,
    pub version: isize,
    pub opaque: i32,
    pub flag: isize,
    pub remark: String,
    pub ext_fields: HashMap<String, String>,
}

pub trait HeaderCodec {
    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error>;
    fn decode(&self, buf: &[u8]) -> Result<Header, Error>;
}

#[derive(Debug, PartialEq)]
pub struct JsonHeaderCodec;

impl HeaderCodec for JsonHeaderCodec {
    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(header)?)
    }
    fn decode(&self, buf: &[u8]) -> Result<Header, Error> {
        Ok(serde_json::from_slice(buf)?)
    }
}

#[derive(Debug, PartialEq)]
pub struct RocketMQHeaderCodec;

impl HeaderCodec for RocketMQHeaderCodec {
    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error> {
        todo!()
    }
    fn decode(&self, buf: &[u8]) -> Result<Header, Error> {
        todo!()
    }
}
