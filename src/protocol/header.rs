use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::io::{Cursor, Read, Write};
use std::str::FromStr;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};

use crate::Error;

pub const HEADER_FIXED_LENGTH: usize = 4;

#[repr(u8)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, IntoPrimitive, TryFromPrimitive,
)]
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
    pub code: i16,
    pub language: LanguageCode,
    pub version: i16,
    pub opaque: i32,
    pub flag: i32,
    #[serde(default)]
    pub remark: String,
    #[serde(default)]
    pub ext_fields: HashMap<String, String>,
}

pub trait HeaderCodec {
    fn codec_type(&self) -> HeaderCodecType;
    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error>;
    fn decode(&self, buf: &[u8]) -> Result<Header, Error>;
}

#[derive(Debug, PartialEq)]
pub struct JsonHeaderCodec;

impl HeaderCodec for JsonHeaderCodec {
    fn codec_type(&self) -> HeaderCodecType {
        HeaderCodecType::Json
    }

    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error> {
        Ok(serde_json::to_vec(header)?)
    }
    fn decode(&self, buf: &[u8]) -> Result<Header, Error> {
        Ok(serde_json::from_slice(buf)?)
    }
}

#[derive(Debug, PartialEq)]
pub struct RocketMQHeaderCodec;

impl RocketMQHeaderCodec {
    fn encode_map(&self, map: &HashMap<String, String>) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        if map.is_empty() {
            return Ok(buf);
        }
        for (key, value) in map.iter() {
            buf.write_i16::<BigEndian>(key.len() as _)?;
            buf.write_all(key.as_bytes())?;
            buf.write_i32::<BigEndian>(value.len() as _)?;
            buf.write_all(value.as_bytes())?;
        }
        Ok(buf)
    }
}

impl HeaderCodec for RocketMQHeaderCodec {
    fn codec_type(&self) -> HeaderCodecType {
        HeaderCodecType::RocketMQ
    }

    fn encode(&self, header: &Header) -> Result<Vec<u8>, Error> {
        let ext_bytes = self.encode_map(&header.ext_fields)?;
        let length = HEADER_FIXED_LENGTH + header.remark.len() + ext_bytes.len();
        let mut buf = Vec::with_capacity(length);
        // request code, 2 bytes
        buf.write_i16::<BigEndian>(header.code as _)?;
        // language flag, 1 byte
        buf.write_u8(LanguageCode::OTHER.into())?;
        // version flag, 2 bytes
        buf.write_i16::<BigEndian>(header.version as _)?;
        // opaque flag, 4 bytes
        buf.write_i32::<BigEndian>(header.opaque)?;
        // request flag, 4 bytes
        buf.write_i32::<BigEndian>(header.flag as _)?;
        // remark length flag, 4 bytes
        buf.write_i32::<BigEndian>(header.remark.len() as _)?;
        if !header.remark.is_empty() {
            // write remark
            buf.write_all(header.remark.as_bytes())?;
        }
        buf.write_i32::<BigEndian>(ext_bytes.len() as _)?;
        if !ext_bytes.is_empty() {
            buf.write_all(&ext_bytes)?;
        }
        Ok(buf)
    }

    fn decode(&self, buf: &[u8]) -> Result<Header, Error> {
        let mut rdr = Cursor::new(buf);
        // request code
        let code = rdr.read_i16::<BigEndian>()?;
        // language flag
        let language = LanguageCode::try_from(rdr.read_u8()?)
            .map_err(|err| Error::InvalidHeader(format!("invalid language: {:?}", err)))?;
        // version flag
        let version = rdr.read_i16::<BigEndian>()?;
        // opaque falg
        let opaque = rdr.read_i32::<BigEndian>()?;
        // request flag
        let flag = rdr.read_i32::<BigEndian>()?;
        // remark
        let remark_len = rdr.read_i32::<BigEndian>()? as usize;
        let remark = if remark_len > 0 {
            let mut remark_bytes = vec![0; remark_len];
            rdr.read_exact(&mut remark_bytes)?;
            String::from_utf8(remark_bytes)?
        } else {
            String::new()
        };
        // ext_fields
        let ext_len = rdr.read_i32::<BigEndian>()? as usize;
        let ext_fields = if ext_len > 0 {
            let mut map = HashMap::new();
            let mut bytes_read = 0;
            while bytes_read < ext_len {
                let key_len = rdr.read_i16::<BigEndian>()? as usize;
                let mut key_bytes = vec![0; key_len];
                rdr.read_exact(&mut key_bytes)?;
                bytes_read += 2 + key_len;

                let val_len = rdr.read_i32::<BigEndian>()? as usize;
                let mut val_bytes = vec![0; val_len];
                rdr.read_exact(&mut val_bytes)?;
                bytes_read += 4 + val_len;

                let key = String::from_utf8(key_bytes)?;
                let val = String::from_utf8(val_bytes)?;
                map.insert(key, val);
            }
            map
        } else {
            HashMap::new()
        };
        let header = Header {
            code,
            language,
            version,
            opaque,
            flag,
            remark,
            ext_fields,
        };
        Ok(header)
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, IntoPrimitive, TryFromPrimitive)]
pub enum HeaderCodecType {
    Json = 0,
    RocketMQ = 1,
}
