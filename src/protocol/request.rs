use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i16)]
#[derive(Debug, Copy, Clone, IntoPrimitive, TryFromPrimitive)]
pub enum RequestCode {
    SendMessage = 10,
    PullMessage = 11,
    QueryConsumerOffset = 14,
    UpdateConsumerOffset = 15,
    SearchOffsetByTimestamp = 29,
    GetMaxOffset = 30,
    Heartbeat = 34,
    ConsumerSendMsgBack = 36,
    EndTransaction = 37,
    GetConsumerListByGroup = 38,
    CheckTransactionState = 39,
    NotifyConsumerIdsChanged = 40,
    LockBatchMQ = 41,
    UnlockBatchMQ = 42,
    GetRouteInfoByTopic = 105,
    ResetConsumerOffset = 220,
    GetConsumerRunningInfo = 307,
    ConsumeMessageDirectly = 309,
    SendBatchMessage = 320,
}
