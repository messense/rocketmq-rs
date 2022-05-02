use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::process;
use std::time::SystemTime;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use flate2::read::ZlibDecoder;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use time::{Date, OffsetDateTime, PrimitiveDateTime, Time};

use crate::utils::client_ip_addr;
use crate::Error;

const NAME_VALUE_SEP: char = '\u{001}';
const PROPERTY_SEP: char = '\u{002}';

static UNIQ_ID_GENERATOR: Lazy<Mutex<UniqueIdGenerator>> = Lazy::new(|| {
    let local_ip = client_ip_addr().unwrap_or_else(|| IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
    let mut buf = Vec::new();
    match local_ip {
        IpAddr::V4(v4) => buf.extend_from_slice(&v4.octets()),
        IpAddr::V6(v6) => buf.extend_from_slice(&v6.octets()),
    };
    buf.write_i16::<BigEndian>(process::id() as i16).unwrap();
    buf.write_i32::<BigEndian>(0).unwrap(); // classLoaderId
    let generator = UniqueIdGenerator {
        counter: 0,
        start_timestamp: 0,
        next_timestamp: 0,
        prefix: hex::encode_upper(buf),
    };
    Mutex::new(generator)
});

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
    pub const SHARDING_KEY: &'static str = "SHARDING_KEY";
}

#[derive(Debug, Clone, Copy, PartialEq, IntoPrimitive, TryFromPrimitive)]
#[repr(i32)]
pub enum MessageSysFlag {
    TransactionNotType = 0,
    Compressed = 0x1,
    MultiTags = 0x2,
    TransactionPreparedType = 0x4,
    TransactionCommitType = 0x8,
    TransactionRollbackType = 0x12,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct MessageQueue {
    pub topic: String,
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "queueId")]
    pub queue_id: u32,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub(crate) topic: String,
    pub(crate) flag: i32,
    pub(crate) sys_flag: i32,
    properties: HashMap<String, String>,
    pub(crate) body: Vec<u8>,
    transaction_id: String,
    pub(crate) batch: bool,
    pub(crate) queue: Option<MessageQueue>,
}

impl Message {
    pub fn new(
        topic: String,
        tags: String,
        keys: String,
        flag: i32,
        body: Vec<u8>,
        wait_store_msg_ok: bool,
    ) -> Message {
        let mut props = HashMap::new();
        if !tags.is_empty() {
            props.insert(Property::TAGS.to_string(), tags);
        }
        if !keys.is_empty() {
            props.insert(Property::KEYS.to_string(), keys);
        }
        if wait_store_msg_ok {
            props.insert(Property::WAIT_STORE_MSG_OK.to_string(), "true".to_string());
        }
        Message {
            topic,
            flag,
            sys_flag: 0,
            body,
            properties: props,
            transaction_id: String::new(),
            batch: false,
            queue: None,
        }
    }

    pub fn unique_key(&self) -> Option<&str> {
        self.properties
            .get(Property::UNIQ_CLIENT_MSG_ID_KEY)
            .and_then(|val| if val.is_empty() { None } else { Some(&val[..]) })
    }

    pub fn set_unique_key(&mut self, unique_key: String) {
        self.properties
            .insert(Property::UNIQ_CLIENT_MSG_ID_KEY.to_string(), unique_key);
    }

    pub fn set_default_unique_key(&mut self) {
        self.properties
            .entry(Property::UNIQ_CLIENT_MSG_ID_KEY.to_string())
            .or_insert_with(|| UNIQ_ID_GENERATOR.lock().generate());
    }

    pub fn get_property(&self, property: &str) -> Option<&String> {
        self.properties.get(property)
    }

    pub fn set_property(&mut self, property: String, value: String) -> Option<String> {
        self.properties.insert(property, value)
    }

    pub fn sharding_key(&self) -> Option<String> {
        self.properties
            .get(Property::SHARDING_KEY)
            .cloned()
            .and_then(|val| if val.is_empty() { None } else { Some(val) })
    }

    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub(crate) fn dump_properties(&self) -> String {
        let mut s = String::new();
        for (k, v) in &self.properties {
            s.reserve(k.len() + v.len() + 2);
            s.push_str(k);
            s.push(NAME_VALUE_SEP);
            s.push_str(v);
            s.push(PROPERTY_SEP);
        }
        s
    }

    fn parse_properties(prop_str: &str) -> HashMap<String, String> {
        let mut props = HashMap::new();
        for item in prop_str.split(PROPERTY_SEP) {
            let kv: Vec<&str> = item.split(NAME_VALUE_SEP).collect();
            if kv.len() == 2 {
                props.insert(kv[0].to_string(), kv[1].to_string());
            }
        }
        props
    }

    fn encode_into(&self, buf: &mut Vec<u8>) -> Result<(), Error> {
        let properties = self.dump_properties();
        let store_size = 4 + 4 + 4 + 4 + 4 + self.body.len() + properties.len();
        // TotalSize Magic BodyCrc Flag BodySize Body PropertySize Property
        buf.reserve(store_size);
        buf.write_u32::<BigEndian>(store_size as u32)?; // 1. TotalSize
        buf.write_u32::<BigEndian>(0)?; // 2. Magic
        buf.write_u32::<BigEndian>(0)?; // 3. BodyCrc
        buf.write_u32::<BigEndian>(self.flag as u32)?; // 4. Flag
        buf.write_u32::<BigEndian>(self.body.len() as u32)?; // 5. BodySize
        buf.write_all(&self.body)?; //  6. Body
        buf.write_u16::<BigEndian>(properties.len() as u16)?; // 7. PropertySize
        buf.write_all(properties.as_bytes())?; // 8. Property
        Ok(())
    }

    pub fn encode_batch(msgs: &[Message]) -> Result<Message, Error> {
        if msgs.is_empty() {
            Err(Error::EmptyBatchMessage)
        } else if msgs.len() == 1 {
            Ok(msgs[0].clone())
        } else {
            let mut body = Vec::new();
            for msg in msgs {
                msg.encode_into(&mut body)?;
            }
            let msg = &msgs[0];
            Ok(Message {
                topic: msg.topic.clone(),
                queue: msg.queue.clone(),
                flag: 0,
                sys_flag: 0,
                properties: HashMap::new(),
                body,
                transaction_id: String::new(),
                batch: true,
            })
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageExt {
    pub(crate) message: Message,
    queue_id: i32,
    store_size: i32,
    queue_offset: i64,
    sys_flag: i32,
    born_host: SocketAddrV4,
    born_timestamp: i64,
    store_host: SocketAddrV4,
    store_timestamp: i64,
    pub(crate) msg_id: String,
    pub(crate) commit_log_offset: i64,
    body_crc: i32,
    reconsume_times: i32,
    prepared_transaction_offset: i64,
}

impl MessageExt {
    pub fn decode(input: &[u8]) -> Vec<Self> {
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
            let physic_offset = rdr.read_i64::<BigEndian>().unwrap();
            let sys_flag = rdr.read_i32::<BigEndian>().unwrap();
            let born_timestamp = rdr.read_i64::<BigEndian>().unwrap();
            let mut born_host_buf = [0u8; 4];
            rdr.read_exact(&mut born_host_buf).unwrap();
            let born_host_port = rdr.read_i32::<BigEndian>().unwrap();
            let born_host = SocketAddrV4::new(
                Ipv4Addr::new(
                    born_host_buf[0],
                    born_host_buf[1],
                    born_host_buf[2],
                    born_host_buf[3],
                ),
                born_host_port as u16,
            );
            let store_timestamp = rdr.read_i64::<BigEndian>().unwrap();
            let mut store_host_buf = [0u8; 4];
            rdr.read_exact(&mut store_host_buf).unwrap();
            let store_host_port = rdr.read_i32::<BigEndian>().unwrap();
            let store_host = SocketAddrV4::new(
                Ipv4Addr::new(
                    store_host_buf[0],
                    store_host_buf[1],
                    store_host_buf[2],
                    store_host_buf[3],
                ),
                store_host_port as u16,
            );

            let reconsume_times = rdr.read_i32::<BigEndian>().unwrap();
            let prepared_transaction_offset = rdr.read_i64::<BigEndian>().unwrap();

            // Body
            let body_len = rdr.read_i32::<BigEndian>().unwrap();
            let body = {
                if body_len > 0 {
                    let mut body = vec![0; body_len as usize];
                    rdr.read_exact(&mut body).unwrap();
                    // decompress
                    if false {
                        let mut decoder = ZlibDecoder::new(&body[..]);
                        let mut body_buf = Vec::new();
                        decoder.read_to_end(&mut body_buf).unwrap();
                        body_buf
                    } else {
                        body
                    }
                } else {
                    Vec::new()
                }
            };

            let topic_len = rdr.read_u8().unwrap();
            let mut topic_buf = vec![0; topic_len as usize];
            rdr.read_exact(&mut topic_buf).unwrap();
            let topic = String::from_utf8(topic_buf).unwrap();

            let properties_len = rdr.read_i16::<BigEndian>().unwrap();
            let properties = {
                if properties_len > 0 {
                    let mut properties_buf = vec![0; properties_len as usize];
                    rdr.read_exact(&mut properties_buf).unwrap();
                    let properties_str = String::from_utf8(properties_buf).unwrap();
                    Message::parse_properties(&properties_str)
                } else {
                    HashMap::new()
                }
            };

            let message = Message {
                topic,
                flag,
                sys_flag: 0,
                properties,
                body,
                transaction_id: String::new(),
                batch: false,
                queue: None,
            };
            let msg_id = message
                .unique_key()
                .map(|key| key.to_string())
                .unwrap_or_else(|| {
                    Self::get_message_offset_id(store_host_buf, store_host_port, physic_offset)
                });
            let msg_ex = MessageExt {
                message,
                queue_id,
                store_size,
                queue_offset,
                sys_flag,
                born_host,
                born_timestamp,
                store_host,
                store_timestamp,
                msg_id,
                commit_log_offset: physic_offset,
                body_crc,
                reconsume_times,
                prepared_transaction_offset,
            };
            msgs.push(msg_ex);
        }
        msgs
    }

    fn get_message_offset_id(store_host: [u8; 4], port: i32, commit_offset: i64) -> String {
        let mut wtr = Vec::new();
        wtr.write_all(&store_host).unwrap();
        wtr.write_i32::<BigEndian>(port).unwrap();
        wtr.write_i64::<BigEndian>(commit_offset).unwrap();
        hex::encode(wtr)
    }
}

struct UniqueIdGenerator {
    counter: i16,
    prefix: String,
    start_timestamp: i64,
    next_timestamp: i64,
}

impl UniqueIdGenerator {
    fn generate(&mut self) -> String {
        if SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
            > self.next_timestamp
        {
            // update timestamp
            let now = OffsetDateTime::now_utc();
            let year = now.year();
            let month = now.month();
            self.start_timestamp = PrimitiveDateTime::new(
                Date::from_calendar_date(year, month, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            )
            .assume_offset(now.offset())
            .unix_timestamp();
            self.next_timestamp = (PrimitiveDateTime::new(
                Date::from_calendar_date(year, month, 1).unwrap(),
                Time::from_hms(0, 0, 0).unwrap(),
            )
            .assume_offset(now.offset())
                + time::Duration::days(30))
            .unix_timestamp();
        }
        self.counter = self.counter.wrapping_add(1);
        let mut buf = Vec::new();
        buf.write_i32::<BigEndian>(
            ((OffsetDateTime::now_utc().unix_timestamp() - self.start_timestamp) * 1000) as i32,
        )
        .unwrap();
        buf.write_i16::<BigEndian>(self.counter).unwrap();
        self.prefix.clone() + &hex::encode(buf)
    }
}

#[cfg(test)]
mod test {
    use super::MessageExt;
    use std::net::{Ipv4Addr, SocketAddrV4};

    #[test]
    fn test_decode_message_ext() {
        let bytes = [
            0, 0, 0, 123, 218, 163, 32, 167, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 123, 0, 0, 0, 0, 0, 1, 226, 64, 0, 0, 0, 0, 0, 0, 1, 104, 106, 154, 142, 143, 127,
            0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 192, 168, 2, 248, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 104, 101, 108, 108, 111, 33, 113, 33, 3, 97, 98,
            99, 0, 21, 97, 1, 49, 50, 51, 2, 98, 1, 104, 101, 108, 108, 111, 2, 99, 1, 51, 46, 49,
            52, 2,
        ];
        let msgs = MessageExt::decode(&bytes[..]);
        assert_eq!(1, msgs.len());
        let msg = &msgs[0];
        assert_eq!("abc", msg.message.topic);
        assert_eq!(b"hello!q!", &msg.message.body[..]);
        assert_eq!(
            SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 0),
            msg.born_host
        );
        assert_eq!(
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 2, 248), 0),
            msg.store_host
        );
        assert_eq!(123456, msg.commit_log_offset);
        assert_eq!(0, msg.prepared_transaction_offset);
        assert_eq!(0, msg.queue_id);
        assert_eq!(123, msg.queue_offset);
        assert_eq!(0, msg.reconsume_times);
        assert_eq!("123", &msg.message.properties["a"]);
        assert_eq!("hello", &msg.message.properties["b"]);
        assert_eq!("3.14", &msg.message.properties["c"]);
    }

    #[test]
    fn text_generate_uniq_id() {
        use super::UNIQ_ID_GENERATOR;
        for i in 0..100 {
            let uid = UNIQ_ID_GENERATOR.lock().generate();
            println!("i: {}, uid: {}", i, uid);
        }
    }
}
