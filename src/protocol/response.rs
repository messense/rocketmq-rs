use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i16)]
#[derive(Debug, Copy, Clone, IntoPrimitive, TryFromPrimitive)]
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
