use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};
use std::sync::atomic::{AtomicIsize, Ordering};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

mod header;

use crate::Error;
use header::{
    Header, HeaderCodec, HeaderCodecType, JsonHeaderCodec, LanguageCode, RocketMQHeaderCodec,
    HEADER_FIXED_LENGTH,
};

const _LENGTH: usize = 4;

static mut GLOBAL_OPAQUE: AtomicIsize = AtomicIsize::new(0);

#[derive(Debug, PartialEq)]
pub struct RemoteCommand {
    header: Header,
    body: Vec<u8>,
}

impl RemoteCommand {
    pub fn new(
        code: i16,
        flag: i32,
        remark: String,
        fields: HashMap<String, String>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            header: Header {
                code: code,
                language: LanguageCode::OTHER,
                version: 431,
                opaque: unsafe { GLOBAL_OPAQUE.fetch_add(1, Ordering::Relaxed) as i32 },
                flag,
                remark,
                ext_fields: fields,
            },
            body,
        }
    }

    fn encode_codec_type(source: i32, codec: impl HeaderCodec) -> [u8; 4] {
        let codec_type: u8 = codec.codec_type().into();
        [
            codec_type,
            ((source >> 16) & 0xff) as u8,
            ((source >> 8) & 0xff) as u8,
            (source & 0xff) as u8,
        ]
    }

    pub fn encode(&self, codec: impl HeaderCodec) -> Result<Vec<u8>, Error> {
        let mut wtr = Vec::new();
        let header_bytes = codec.encode(&self.header)?;
        let header_len = header_bytes.len();
        let length = HEADER_FIXED_LENGTH + header_len + self.body.len();
        wtr.write_i32::<BigEndian>(length as i32)?;
        let header_len_with_codec = Self::encode_codec_type(header_len as i32, codec);
        wtr.write_all(&header_len_with_codec)?;
        wtr.write_all(&header_bytes)?;
        if !self.body.is_empty() {
            wtr.write_all(&self.body)?;
        }
        Ok(wtr)
    }

    pub fn decode(input: &[u8]) -> Result<Self, Error> {
        let mut rdr = Cursor::new(input);
        let length = rdr.read_i32::<BigEndian>()?;
        let origin_header_len = rdr.read_i32::<BigEndian>()?;
        let header_len = origin_header_len & 0xffffff;
        let mut header_buf = vec![0; header_len as usize];
        rdr.read_exact(&mut header_buf)?;
        let codec_type = HeaderCodecType::try_from(((origin_header_len >> 24) & 0xff) as u8)
            .map_err(|_| Error::InvalidHeaderCodec)?;
        let header = match codec_type {
            HeaderCodecType::Json => {
                let codec = JsonHeaderCodec;
                codec.decode(&header_buf)?
            }
            HeaderCodecType::RocketMQ => {
                let codec = RocketMQHeaderCodec;
                codec.decode(&header_buf)?
            }
        };
        let body_len = length as usize - HEADER_FIXED_LENGTH - header_len as usize;
        let body = {
            if body_len > 0 {
                let mut body_buf = vec![0; body_len];
                rdr.read_exact(&mut body_buf)?;
                body_buf
            } else {
                Vec::new()
            }
        };
        Ok(Self { header, body })
    }
}

pub struct RequestCode;

impl RequestCode {
    pub const SEND_MESSAGE: isize = 10;
}

#[cfg(test)]
mod test {
    use super::header::{JsonHeaderCodec, RocketMQHeaderCodec};
    use super::RemoteCommand;
    use std::collections::HashMap;

    #[test]
    fn test_remote_command_json_encode_decode_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let cmd = RemoteCommand::new(10, 0, "remark".to_string(), fields, b"Hello World".to_vec());
        let encoded = cmd.encode(JsonHeaderCodec).unwrap();
        let decoded = RemoteCommand::decode(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_remote_command_rocketmq_encode_decode_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let cmd = RemoteCommand::new(10, 0, "remark".to_string(), fields, b"Hello World".to_vec());
        let encoded = cmd.encode(RocketMQHeaderCodec).unwrap();
        let decoded = RemoteCommand::decode(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }
}
