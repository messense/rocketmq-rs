use std::collections::HashMap;

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
