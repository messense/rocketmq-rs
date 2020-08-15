use std::convert::TryFrom;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::Error;

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum ResponseCode {
    /// success response from broker
    Success = 0,
    /// exception from broker
    SystemError = 1,
    /// system busy from broker
    SystemBusy = 2,
    /// broker don't support the request code
    RequestCodeNotSupported = 3,
    /// broker flush disk timeout error
    FlushDiskTimeout = 10,
    /// broker sync double write, slave broker not available
    SlaveNotAvailable = 11,
    /// broker sync double write, slave broker flush msg timeout
    FlushSlaveTimeout = 12,
    /// broker received illegal message
    MessageIllegal = 13,
    /// service not available due to broker or namesrv in shutdown status
    ServiceNotAvailable = 14,
    /// this version is not supported on broker or namesrv
    VersionNotSupported = 15,
    /// broker or namesrv has no permission to do this operation
    NoPermission = 16,
    /// topic is not exist on broker
    TopicNotExist = 17,
    /// broker already created this topic
    TopicExistAlready = 18,
    /// pulled message was not found
    PullNotFound = 19,
    /// retry later
    PullRetryImmediately = 20,
    /// pull message with wrong offset
    PullOffsetMoved = 21,
    /// could not find the query message
    QueryNotFound = 22,
    SubscriptionParseFailed = 23,
    SubscriptionNotExist = 24,
    SubscriptionNotLatest = 25,
    SubscriptionGroupNotExist = 26,
    TransactionShouldCommit = 200,
    TransactionShouldRollback = 201,
    TransactionStateUnknown = 202,
    TransactionStateGroupWrong = 203,
    ConsumerNotOnline = 206,
    ConsumeMsgTimeout = 207,
}

impl ResponseCode {
    pub fn from_code(code: i16) -> Result<Self, Error> {
        ResponseCode::try_from(code).map_err(|_| Error::ResponseError {
            code,
            message: format!("unknown response code {}", code),
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
