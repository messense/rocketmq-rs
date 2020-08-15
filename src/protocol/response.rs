use std::convert::TryFrom;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::Error;

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum ResponseCode {
    Success = 0,
    Error = 1,
    FlushDiskTimeout = 10,
    SlaveNotAvailable = 11,
    FlushSlaveTimeout = 12,
    TopicNotExist = 17,
    PullNotFound = 19,
    PullRetryImmediately = 20,
    PullOffsetMoved = 21,
}

impl ResponseCode {
    pub fn from_code(code: i16) -> Result<Self, Error> {
        ResponseCode::try_from(code).map_err(|_| Error::ResponseError {
            code,
            message: format!("invalid response code {}", code),
        })
    }
}

impl PartialEq<ResponseCode> for i16 {
    fn eq(&self, other: &ResponseCode) -> bool {
        *self == *other as i16
    }
}

#[derive(Debug, Clone)]
pub struct SendMessageResponse {
    pub msg_id: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub transaction_id: String,
    pub msg_region: String,
}

#[derive(Debug, Clone)]
pub struct PullMessageResponse {
    pub suggest_which_broker_id: i64,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
}
