use std::collections::HashMap;

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

pub trait EncodeRequestHeader {
    fn encode(self) -> HashMap<String, String>;
}

#[derive(Debug, Clone)]
pub struct SendMessageRequestHeader {
    pub producer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: String,
    pub reconsume_times: i32,
    pub unit_mode: bool,
    pub max_reconsume_times: i32,
    pub batch: bool,
    pub default_topic: String,
    pub default_topic_queue_nums: String,
}

impl EncodeRequestHeader for SendMessageRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("producerGroup".to_string(), self.producer_group);
        map.insert("topic".to_string(), self.topic);
        map.insert("queueId".to_string(), self.queue_id.to_string());
        map.insert("sysFlag".to_string(), self.sys_flag.to_string());
        map.insert("bornTimestamp".to_string(), self.born_timestamp.to_string());
        map.insert("flag".to_string(), self.flag.to_string());
        map.insert(
            "reconsumeTimes".to_string(),
            self.reconsume_times.to_string(),
        );
        map.insert("unitMode".to_string(), self.flag.to_string());
        map.insert(
            "maxReconsumeTimes".to_string(),
            self.max_reconsume_times.to_string(),
        );
        map.insert("defaultTopic".to_string(), self.default_topic);
        map.insert(
            "defaultTopicQueueNums".to_string(),
            self.default_topic_queue_nums.to_string(),
        );
        map.insert("batch".to_string(), self.batch.to_string());
        map.insert("properties".to_string(), self.properties);
        map
    }
}

#[derive(Debug, Clone)]
pub struct CheckTransactionStateRequestHeader {
    pub tran_state_table_offset: i64,
    pub commit_log_offset: i64,
    pub msg_id: String,
    pub transaction_id: String,
    pub offset_msg_id: String,
}

impl EncodeRequestHeader for CheckTransactionStateRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert(
            "tranStateTableOffset".to_string(),
            self.tran_state_table_offset.to_string(),
        );
        map.insert(
            "commitLogOffset".to_string(),
            self.commit_log_offset.to_string(),
        );
        map.insert("msgId".to_string(), self.msg_id);
        map.insert("transactionId".to_string(), self.transaction_id);
        map.insert("offsetMsgId".to_string(), self.offset_msg_id);
        map
    }
}
