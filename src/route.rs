use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "readQueueNums")]
    read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    write_queue_nums: i32,
    perm: i32,
    #[serde(rename = "topicSyncFlag")]
    topic_sync_flag: i32,
}

#[derive(Debug, Serialize)]
pub struct BrokerData {
    cluster: String,
    #[serde(rename = "brokerName")]
    broker_name: String,
    #[serde(rename = "brokerAddrs")]
    broker_addrs: HashMap<i64, String>,
}

#[derive(Debug, Serialize)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    order_topic_conf: String,
    #[serde(rename = "queueDatas")]
    queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    filter_server_table: HashMap<String, Vec<String>>,
}