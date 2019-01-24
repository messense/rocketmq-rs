use std::collections::HashMap;

use rand::prelude::*;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "readQueueNums")]
    read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,
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

impl BrokerData {
    /// Selects a (preferably master) broker address from the registered list.
    /// If the master's address cannot be found, a slave broker address is selected in a random manner.
    pub fn select_broker_addr(&self) -> &str {
        self.broker_addrs
            .get(&0)
            .or_else(|| {
                let addrs: Vec<_> = self.broker_addrs.values().collect();
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0, addrs.len());
                addrs.get(index).cloned()
            })
            .expect("No broker found")
    }
}

#[derive(Debug, Serialize)]
pub struct TopicRouteData {
    #[serde(rename = "orderTopicConf")]
    order_topic_conf: String,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    filter_server_table: HashMap<String, Vec<String>>,
}
