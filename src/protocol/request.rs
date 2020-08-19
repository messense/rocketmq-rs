use std::collections::HashMap;
use std::time::Duration;

use num_enum::{IntoPrimitive, TryFromPrimitive};

#[repr(i16)]
#[derive(Debug, Copy, Clone, PartialEq, IntoPrimitive, TryFromPrimitive)]
pub enum RequestCode {
    /// send message to broker
    SendMessage = 10,
    /// subscribe message from broker
    PullMessage = 11,
    /// query message from broker
    QueryMessage = 12,
    /// query broker offset
    QueryBrokerOffset = 13,
    /// query consumer offset from broker
    QueryConsumerOffset = 14,
    /// update consumer offset to broker
    UpdateConsumerOffset = 15,
    /// create or update topic to broker
    UpdateAndCreateTopic = 17,
    /// get all topic config info from broker
    GetAllTopicConfig = 21,
    /// get all topic list from broker
    GetTopicConfigList = 22,
    /// get topic name list from broker
    GetTopicNameList = 23,
    UpdateBrokerConfig = 25,
    GetBrokerConfig = 26,
    TriggerDeleteFiles = 27,
    GetBrokerRuntimeInfo = 28,
    SearchOffsetByTimestamp = 29,
    GetMaxOffset = 30,
    GetMinOffset = 31,
    GetEarliestMsgStoreTime = 32,
    ViewMessageById = 33,
    /// send heartbeat to broker and register itself
    Heartbeat = 34,
    /// unregister client from broker
    UnregisterClient = 35,
    /// send back consume failed message to broker
    ConsumerSendMsgBack = 36,
    /// commit or rollback transaction
    EndTransaction = 37,
    /// get consumer list by group from broker
    GetConsumerListByGroup = 38,
    CheckTransactionState = 39,
    /// broker send notify to consumer when consumer lists changes
    NotifyConsumerIdsChanged = 40,
    /// lock mq before orderly consume
    LockBatchMQ = 41,
    /// unlock mq after orderly consume
    UnlockBatchMQ = 42,
    GetAllConsumerOffset = 43,
    GetAllDelayOffset = 45,
    PutKvConfig = 100,
    GetKvConfig = 101,
    DeleteKvConfig = 102,
    RegisterBroker = 103,
    UnregisterBroker = 104,
    GetRouteInfoByTopic = 105,
    GetBrokerClusterInfo = 106,
    UpdateAndCreateSubscriptionGroup = 200,
    GetAllSubscriptionGroupConfig = 201,
    GetTopicStatsInfo = 202,
    GetConsumerConnectionList = 203,
    GetProducerConnectionList = 204,
    WipeWritePermOfBroker = 205,
    GetAllTopicListFromNameServer = 206,
    DeleteSubscriptionGroup = 207,
    GetConsumeStats = 208,
    SuspendConsumer = 209,
    ResumeConsumer = 210,
    ResetConsumerOffsetInConsumer = 211,
    ResetConsumerOffsetInBroker = 212,
    AdjustConsumerThreadPool = 213,
    WhoConsumeTheMessage = 214,
    DeleteTopicInBroker = 215,
    DeleteTopicInNameServer = 216,
    GetKvConfigByValue = 217,
    DeleteKvConfigByValue = 218,
    GetKvListByNamespace = 219,
    ResetConsumerClientOffset = 220,
    GetConsumerStatusFromClient = 221,
    InvokeBrokerToResetOffset = 222,
    InvokeBrokerToGetConsumerStatus = 223,
    QueryTopicsByCluster = 224,
    RegisterFilterServer = 301,
    RegisterMessageFilterClass = 302,
    QueryConsumeTimeSpan = 303,
    GetSystemTopicListFromNameServer = 304,
    GetSystemTopicListFromBroker = 305,
    CleanExpiredConsumeQueue = 306,
    GetConsumerRunningInfo = 307,
    QueryConnectionOffset = 308,
    ConsumeMessageDirectly = 309,
    SendMessageV2 = 310,
    GetUnitTopicList = 311,
    GetHasUnitSubTopicList = 312,
    GetHasUnitSubUnunitTopicList = 313,
    CloneGroupOffset = 314,
    ViewBrokerStatsData = 315,
    SendBatchMessage = 320,
}

pub trait EncodeRequestHeader {
    fn encode(self) -> HashMap<String, String>;
}

impl EncodeRequestHeader for HashMap<String, String> {
    fn encode(self) -> HashMap<String, String> {
        self
    }
}

#[derive(Debug, Clone)]
pub struct SendMessageRequestHeader {
    pub producer_group: String,
    pub topic: String,
    pub queue_id: u32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: String,
    pub reconsume_times: i32,
    pub unit_mode: bool,
    pub max_reconsume_times: i32,
    pub batch: bool,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
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
        map.insert("unitMode".to_string(), self.unit_mode.to_string());
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
pub struct SendMessageRequestV2Header {
    pub producer_group: String,
    pub topic: String,
    pub queue_id: u32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub flag: i32,
    pub properties: String,
    pub reconsume_times: i32,
    pub unit_mode: bool,
    pub max_reconsume_times: i32,
    pub batch: bool,
    pub default_topic: String,
    pub default_topic_queue_nums: i32,
}

impl EncodeRequestHeader for SendMessageRequestV2Header {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("a".to_string(), self.producer_group);
        map.insert("b".to_string(), self.topic);
        map.insert("c".to_string(), self.default_topic);
        map.insert("d".to_string(), self.default_topic_queue_nums.to_string());
        map.insert("e".to_string(), self.queue_id.to_string());
        map.insert("f".to_string(), self.sys_flag.to_string());
        map.insert("g".to_string(), self.born_timestamp.to_string());
        map.insert("h".to_string(), self.flag.to_string());
        map.insert("i".to_string(), self.properties);
        map.insert("j".to_string(), self.reconsume_times.to_string());
        map.insert("k".to_string(), self.unit_mode.to_string());
        map.insert("l".to_string(), self.max_reconsume_times.to_string());
        map.insert("m".to_string(), self.batch.to_string());
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

#[derive(Debug, Clone)]
pub struct GetRouteInfoRequestHeader {
    pub topic: String,
}

impl EncodeRequestHeader for GetRouteInfoRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), self.topic);
        map
    }
}

#[derive(Debug, Clone)]
pub struct PullMessageRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub max_msg_nums: i32,
    pub sys_flag: i32,
    pub commit_offset: i64,
    pub suspend_timeout_millis: Duration,
    pub sub_expression: String,
    pub sub_version: i64,
    pub expression_type: String,
}

impl EncodeRequestHeader for PullMessageRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("consumerGroup".to_string(), self.consumer_group);
        map.insert("topic".to_string(), self.topic);
        map.insert("queueId".to_string(), self.queue_id.to_string());
        map.insert("queueOffset".to_string(), self.queue_offset.to_string());
        map.insert("maxMsgNums".to_string(), self.max_msg_nums.to_string());
        map.insert("sysFlag".to_string(), self.sys_flag.to_string());
        map.insert("commitOffset".to_string(), self.commit_offset.to_string());
        map.insert(
            "suspendTimeoutMillis".to_string(),
            self.suspend_timeout_millis.as_millis().to_string(),
        );
        map.insert("subscription".to_string(), self.sub_expression);
        map.insert("subVersion".to_string(), self.sub_version.to_string());
        map.insert("expressionType".to_string(), self.expression_type);
        map
    }
}

#[derive(Debug, Clone)]
pub struct UnregisterClientRequestHeader {
    pub client_id: String,
    pub producer_group: String,
    pub consumer_group: String,
}

impl EncodeRequestHeader for UnregisterClientRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("clientID".to_string(), self.client_id);
        map.insert("producerGroup".to_string(), self.producer_group);
        map.insert("consumerGroup".to_string(), self.consumer_group);
        map
    }
}

#[derive(Debug, Clone)]
pub struct CreateTopicRequestHeader {
    pub topic: String,
    pub default_topic: String,
    pub read_queue_nums: u32,
    pub write_queue_nums: u32,
    pub permission: i32,
    pub topic_filter_type: String,
    pub topic_sys_flag: i32,
    pub order: bool,
}

impl EncodeRequestHeader for CreateTopicRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), self.topic);
        map.insert("defaultTopic".to_string(), self.default_topic);
        map.insert(
            "readQueueNums".to_string(),
            self.read_queue_nums.to_string(),
        );
        map.insert(
            "writeQueueNums".to_string(),
            self.write_queue_nums.to_string(),
        );
        map.insert("perm".to_string(), self.permission.to_string());
        map.insert("topicFilterType".to_string(), self.topic_filter_type);
        map.insert("topicSysFlag".to_string(), self.topic_sys_flag.to_string());
        map.insert("order".to_string(), self.order.to_string());
        map
    }
}

#[derive(Debug, Clone)]
pub struct QueryConsumerOffsetRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: u32,
}

impl EncodeRequestHeader for QueryConsumerOffsetRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("consumerGroup".to_string(), self.consumer_group);
        map.insert("topic".to_string(), self.topic);
        map.insert("queueId".to_string(), self.queue_id.to_string());
        map
    }
}

#[derive(Debug, Clone)]
pub struct UpdateConsumerOffsetRequestHeader {
    pub consumer_group: String,
    pub topic: String,
    pub queue_id: u32,
    pub commit_offset: i64,
}

impl EncodeRequestHeader for UpdateConsumerOffsetRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("consumerGroup".to_string(), self.consumer_group);
        map.insert("topic".to_string(), self.topic);
        map.insert("queueId".to_string(), self.queue_id.to_string());
        map.insert("commitOffset".to_string(), self.commit_offset.to_string());
        map
    }
}

#[derive(Debug, Clone)]
pub struct GetConsumerListRequestHeader {
    pub consumer_group: String,
}

impl EncodeRequestHeader for GetConsumerListRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("consumerGroup".to_string(), self.consumer_group);
        map
    }
}

#[derive(Debug, Clone)]
pub struct GetMaxOffsetRequestHeader {
    pub topic: String,
    pub queue_id: u32,
}

impl EncodeRequestHeader for GetMaxOffsetRequestHeader {
    fn encode(self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("topic".to_string(), self.topic);
        map.insert("queueId".to_string(), self.queue_id.to_string());
        map
    }
}
