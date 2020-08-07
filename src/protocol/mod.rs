use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{Cursor, Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

mod header;
pub mod request;
pub mod response;

use crate::Error;
use header::{Header, HeaderCodec, LanguageCode, HEADER_FIXED_LENGTH};
pub use header::{HeaderCodecType, JsonHeaderCodec, RocketMQHeaderCodec};

const _LENGTH: usize = 4;
const RESPONSE_TYPE: i32 = 1;

#[derive(Debug, PartialEq)]
pub struct RemoteCommand {
    pub(crate) header: Header,
    body: Vec<u8>,
}

impl RemoteCommand {
    pub fn new(
        opaque: i32,
        code: i16,
        flag: i32,
        remark: String,
        fields: HashMap<String, String>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            header: Header {
                code,
                language: LanguageCode::OTHER,
                version: 431,
                opaque,
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

    pub fn code(&self) -> i16 {
        self.header.code
    }

    pub fn is_response_type(&self) -> bool {
        self.header.flag & RESPONSE_TYPE == RESPONSE_TYPE
    }

    pub fn mark_response_type(&mut self) {
        self.header.flag |= RESPONSE_TYPE
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

#[derive(Debug, Clone)]
pub(crate) struct MqCodec;

impl Encoder<RemoteCommand> for MqCodec {
    type Error = Error;

    fn encode(&mut self, item: RemoteCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        // FIXME: write into dst directly
        let bytes = item.encode(RocketMQHeaderCodec)?;
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

impl Decoder for MqCodec {
    type Item = RemoteCommand;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let item = RemoteCommand::decode(&src)?;
        Ok(Some(item))
    }
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
        let cmd = RemoteCommand::new(
            1,
            10,
            0,
            "remark".to_string(),
            fields,
            b"Hello World".to_vec(),
        );
        let encoded = cmd.encode(JsonHeaderCodec).unwrap();
        let decoded = RemoteCommand::decode(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_remote_command_rocketmq_encode_decode_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let cmd = RemoteCommand::new(
            1,
            10,
            0,
            "remark".to_string(),
            fields,
            b"Hello World".to_vec(),
        );
        let encoded = cmd.encode(RocketMQHeaderCodec).unwrap();
        let decoded = RemoteCommand::decode(&encoded).unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_remote_command_type() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let mut cmd = RemoteCommand::new(
            1,
            10,
            0,
            "remark".to_string(),
            fields,
            b"Hello World".to_vec(),
        );
        assert!(!cmd.is_response_type());

        cmd.mark_response_type();
        assert!(cmd.is_response_type());
    }
}
