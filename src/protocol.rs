use std::collections::HashMap;
use std::io::Write;

use serde::Serialize;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const _HEADER_LENGTH: usize = 4;

#[derive(Debug, Serialize)]
pub struct Header {
    code: isize,
    language: String,
    version: isize,
    opaque: i32,
    flag: isize,
    remark: String,
    ext_fields: HashMap<String, String>,
}

#[derive(Debug)]
pub struct RemoteCommand {
    length: i32,
    header_length: i32,
    header: Header,
    body: Vec<u8>,
}

impl RemoteCommand {
    pub fn encode(&self) -> Vec<u8> {
        let mut wtr = Vec::new();
        let header_bytes = serde_json::to_vec(&self.header).unwrap();
        let header_len = header_bytes.len();
        let length = _HEADER_LENGTH + header_len + self.body.len();
        wtr.write_i32::<BigEndian>(length as i32).unwrap();
        wtr.write_i32::<BigEndian>(header_len as i32).unwrap();
        wtr.write_all(&header_bytes);
        if !self.body.is_empty() {
            wtr.write_all(&self.body).unwrap();
        }
        wtr
    }

    pub fn from_buffer(input: &[u8]) -> Self {
        unimplemented!()
    }
}