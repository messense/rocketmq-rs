
use std::collections::HashMap;
use std::io::{Read, Cursor};

use byteorder::{BigEndian, ReadBytesExt};
use flate2::read::ZlibDecoder;

pub struct Property;

impl Property {
    pub const KEYS: &'static str = "KEYS";
    pub const TAGS: &'static str = "TAGS";
    pub const WAIT_STORE_MSG_OK: &'static str = "WAIT";
    pub const DELAY_TIME_LEVEL: &'static str = "DELAY";
    pub const RETRY_TOPIC: &'static str = "RETRY_TOPIC";
    pub const REAL_TOPIC: &'static str = "REAL_TOPIC";
    pub const REAL_QUEUE_ID: &'static str = "REAL_QID";
    pub const TRANSACTION_PREPARED: &'static str = "TRAN_MSG";
    pub const PRODUCER_GROUP: &'static str = "PGROUP";
    pub const MIN_OFFSET: &'static str = "MIN_OFFSET";
    pub const MAX_OFFSET: &'static str = "MAX_OFFSET";
    pub const BUYER_ID: &'static str = "BUYER_ID";
    pub const ORIGIN_MESSAGE_ID: &'static str = "ORIGIN_MESSAGE_ID";
    pub const TRANSFER_FLAG: &'static str = "TRANSFER_FLAG";
    pub const CORRECTION_FLAG: &'static str = "CORRECTION_FLAG";
    pub const MQ2_FLAG: &'static str = "MQ2_FLAG";
    pub const RECONSUME_TIME: &'static str = "RECONSUME_TIME";
    pub const MSG_REGION: &'static str = "MSG_REGION";
    pub const TRACE_SWITCH: &'static str = "TRACE_ON";
    pub const UNIQ_CLIENT_MSG_ID_KEY: &'static str = "UNIQ_KEY";
    pub const MAX_RECONSUME_TIMES: &'static str = "MAX_RECONSUME_TIMES";
    pub const TRANSACTION_PREPARED_QUEUE_OFFSET: &'static str = "TRAN_PREPARED_QUEUE_OFFSET";
    pub const TRANSACTION_CHECK_TIMES: &'static str = "TRANSACTION_CHECK_TIMES";
    pub const CHECK_IMMUNITY_TIME_IN_SECONDS: &'static str = "CHECK_IMMUNITY_TIME_IN_SECONDS";
    pub const KEY_SEPARATOR: &'static str = " ";
}

pub struct MessageQueue {
    topic: String,
    broker_name: String,
    queue_id: u32,
}

pub struct Message {
    topic: String,
    flag: i32,
    properties: HashMap<&'static str, String>,
    body: Vec<u8>,
    transaction_id: String,
}

impl Message {
    pub fn new(topic: String, tags: String, keys: String, flag: i32, body: Vec<u8>, wait_store_msg_ok: bool) -> Message {
        let mut props = HashMap::new();
        if !tags.is_empty() {
            props.insert(Property::TAGS, tags);
        }
        if !keys.is_empty() {
            props.insert(Property::KEYS, keys);
        }
        if wait_store_msg_ok {
            props.insert(Property::WAIT_STORE_MSG_OK, "true".to_string());
        } else {
            props.insert(Property::WAIT_STORE_MSG_OK, "false".to_string());
        }
        Message {
            topic,
            flag,
            body,
            properties: props,
            transaction_id: String::new(),
        }
    }
}

pub struct MessageExt {
    message: Message,
    queue_id: i32,
    store_size: i32,
    queue_offset: i64,
    sys_flag: i32,
    born_timestamp: i64,
    store_timestamp: i64,
    msg_id: String,
    commit_log_offset: i64,
    body_crc: i32,
    reconsume_times: i32,
    prepared_transaction_offset: i64,
}

impl MessageExt {
    pub fn from_buffer(input: &[u8]) -> Vec<Self> {
        let input_len = input.len() as u64;
        let mut rdr = Cursor::new(input);
        let mut msgs = Vec::new();
        while rdr.position() < input_len {
            let store_size = rdr.read_i32::<BigEndian>().unwrap();
            let magic_code = rdr.read_i32::<BigEndian>().unwrap();
            if magic_code != -626843481 {
                // TODO: check
            }
            let body_crc = rdr.read_i32::<BigEndian>().unwrap();
            let queue_id = rdr.read_i32::<BigEndian>().unwrap();
            let flag = rdr.read_i32::<BigEndian>().unwrap();
            let queue_offset = rdr.read_i64::<BigEndian>().unwrap();
            let physic_offset  = rdr.read_i64::<BigEndian>().unwrap();
            let sys_flag  = rdr.read_i32::<BigEndian>().unwrap();
            let born_timestamp = rdr.read_i64::<BigEndian>().unwrap();
            let mut born_host_buf = [0u8; 4];
            rdr.read_exact(&mut born_host_buf).unwrap();
            let born_host_port = rdr.read_i32::<BigEndian>().unwrap();
            let store_timestamp = rdr.read_i64::<BigEndian>().unwrap();
            let mut store_host_buf = [0u8; 4];
            rdr.read_exact(&mut store_host_buf).unwrap();
            let store_host_port = rdr.read_i32::<BigEndian>().unwrap();
            let reconsume_times = rdr.read_i32::<BigEndian>().unwrap();
            let prepared_transaction_offset = rdr.read_i64::<BigEndian>().unwrap();

            // Body
            let body_len = rdr.read_i32::<BigEndian>().unwrap();
            if body_len > 0 {
                let mut body = Vec::with_capacity(body_len as usize);
                rdr.read_exact(&mut body).unwrap();
                // decompress
                let body = {
                    if true {
                        let mut decoder = ZlibDecoder::new(&body[..]);
                        let mut body_buf = Vec::new();
                        decoder.read_to_end(&mut body_buf).unwrap();
                        body_buf
                    } else {
                        body
                    }
                };
            }

            let topic_len = rdr.read_u8().unwrap();
            let mut topic_buf = Vec::with_capacity(topic_len as usize);
            rdr.read_exact(&mut topic_buf).unwrap();
            let topic = String::from_utf8(topic_buf).unwrap();

            let properties_len = rdr.read_i16::<BigEndian>().unwrap();
            if properties_len > 0 {
                let mut properties_buf = Vec::with_capacity(properties_len as usize);
                rdr.read_exact(&mut properties_buf).unwrap();
                let properties_str = String::from_utf8(properties_buf).unwrap();
                let properties = Self::parse_properties(&properties_str);
            }

        }
        msgs
    }

    fn parse_properties(prop_str: &str) -> HashMap<String, String> {
        let mut props = HashMap::new();
        for item in prop_str.split('2') {
            let kv: Vec<&str> = item.split('1').collect();
            if kv.len() == 2 {
                props.insert(kv[0].to_string(), kv[1].to_string());
            }
        }
        props
    }
}