use std::collections::HashMap;
use std::io::{Read, Write, Cursor};

use serde::{Serialize, Deserialize};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const _LENGTH: usize = 4;
const _HEADER_LENGTH: usize = 4;

#[derive(Debug, Serialize, Deserialize)]
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
        let mut rdr = Cursor::new(input);
        let length = rdr.read_i32::<BigEndian>().unwrap();
        let header_len = rdr.read_i32::<BigEndian>().unwrap();
        let mut header_buf = Vec::with_capacity(header_len as usize);
        rdr.read_exact(&mut header_buf).unwrap();
        let header: Header = serde_json::from_slice(&header_buf).unwrap();
        let body_len = length as usize - _HEADER_LENGTH - header_len as usize;
        let body = {
            if body_len > 0 {
                let mut body_buf = Vec::with_capacity(body_len);
                rdr.read_exact(&mut body_buf).unwrap();
                body_buf
            } else {
                Vec::new()
            }
        };
        Self {
            length,
            header_length: header_len,
            header,
            body,
        }
    }
}