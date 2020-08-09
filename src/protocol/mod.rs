use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Read;

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

mod header;
pub mod request;
pub mod response;

use crate::Error;
use header::{Header, HeaderCodec, LanguageCode, HEADER_FIXED_LENGTH};
pub use header::{HeaderCodecType, JsonHeaderCodec, RocketMQHeaderCodec};
use request::EncodeRequestHeader;

const _LENGTH: usize = 4;
const RESPONSE_TYPE: i32 = 1;

#[derive(Debug, PartialEq)]
pub struct RemotingCommand {
    pub(crate) header: Header,
    pub(crate) body: Vec<u8>,
}

impl RemotingCommand {
    pub fn new(
        opaque: i32,
        code: i16,
        flag: i32,
        remark: String,
        ext_fields: HashMap<String, String>,
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
                ext_fields,
            },
            body,
        }
    }

    pub fn with_header<H: EncodeRequestHeader>(code: i16, header: H, body: Vec<u8>) -> Self {
        let ext_fields = header.encode();
        Self::new(0, code, 0, String::new(), ext_fields, body)
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

    fn encode_into(&self, wtr: &mut BytesMut, codec: impl HeaderCodec) -> Result<(), Error> {
        let header_bytes = codec.encode(&self.header)?;
        let header_len = header_bytes.len();
        let length = HEADER_FIXED_LENGTH + header_len + self.body.len();
        wtr.put_i32(length as i32);
        let header_len_with_codec = Self::encode_codec_type(header_len as i32, codec);
        wtr.put(&header_len_with_codec[..]);
        wtr.put(&header_bytes[..]);
        if !self.body.is_empty() {
            wtr.put(&self.body[..]);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MqCodec;

impl Encoder<RemotingCommand> for MqCodec {
    type Error = Error;

    fn encode(&mut self, item: RemotingCommand, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode_into(dst, RocketMQHeaderCodec)?;
        Ok(())
    }
}

impl Decoder for MqCodec {
    type Item = RemotingCommand;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut buf = &src[0..src.len()];
        if buf.len() < HEADER_FIXED_LENGTH {
            src.reserve(HEADER_FIXED_LENGTH);
            return Ok(None);
        }
        let length = buf.read_i32::<BigEndian>()?;
        if buf.len() < length as usize {
            src.reserve(length as usize);
            return Ok(None);
        }
        let origin_header_len = buf.read_i32::<BigEndian>()?;
        let header_len = origin_header_len & 0xffffff;
        let mut header_buf = vec![0; header_len as usize];
        buf.read_exact(&mut header_buf)?;
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
                buf.read_exact(&mut body_buf)?;
                body_buf
            } else {
                Vec::new()
            }
        };
        src.advance(HEADER_FIXED_LENGTH + length as usize);
        Ok(Some(RemotingCommand { header, body }))
    }
}

#[cfg(test)]
mod test {
    use super::header::{JsonHeaderCodec, RocketMQHeaderCodec};
    use super::{MqCodec, RemotingCommand};
    use bytes::BytesMut;
    use std::collections::HashMap;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_remote_command_json_encode_decode_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let cmd = RemotingCommand::new(
            1,
            10,
            0,
            "remark".to_string(),
            fields,
            b"Hello World".to_vec(),
        );
        let mut encoded = BytesMut::new();
        cmd.encode_into(&mut encoded, JsonHeaderCodec).unwrap();
        let mut decoder = MqCodec;
        let decoded = decoder.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_remote_command_rocketmq_encode_decode_roundtrip() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let cmd = RemotingCommand::new(
            1,
            10,
            0,
            "remark".to_string(),
            fields,
            b"Hello World".to_vec(),
        );
        let mut encoded = BytesMut::new();
        cmd.encode_into(&mut encoded, RocketMQHeaderCodec).unwrap();
        let mut decoder = MqCodec;
        let decoded = decoder.decode(&mut encoded).unwrap().unwrap();
        assert_eq!(cmd, decoded);
    }

    #[test]
    fn test_remote_command_type() {
        let mut fields = HashMap::new();
        fields.insert("messageId".to_string(), "123".to_string());
        fields.insert("offset".to_string(), "456".to_string());
        let mut cmd = RemotingCommand::new(
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
