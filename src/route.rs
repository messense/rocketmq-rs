use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::prelude::*;
use serde::Deserialize;

use crate::message::MessageQueue;
use crate::Error;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,
    pub perm: i32,
    #[serde(default, rename = "topicSyncFlag")]
    pub topic_sync_flag: i32,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct BrokerData {
    pub cluster: String,
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "brokerAddrs")]
    pub broker_addrs: HashMap<i64, String>,
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

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct TopicRouteData {
    #[serde(default, rename = "orderTopicConf")]
    pub order_topic_conf: String,
    #[serde(rename = "queueDatas")]
    pub queue_datas: Vec<QueueData>,
    #[serde(rename = "brokerDatas")]
    pub broker_datas: Vec<BrokerData>,
    #[serde(rename = "filterServerTable")]
    pub filter_server_table: HashMap<String, Vec<String>>,
}

impl TopicRouteData {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let mut s = String::from_utf8(bytes.to_vec()).unwrap();
        // fixup fastjson mess
        s = s.replace(",0:", ",\"0\":");
        s = s.replace(",1:", ",\"1\":");
        s = s.replace("{0:", "{\"0\":");
        s = s.replace("{1:", "{\"1\":");
        let data: TopicRouteData = serde_json::from_str(&s)?;
        Ok(data)
    }
}

pub struct TopicPublishInfo {
    order_topic: bool,
    have_topic_router_info: bool,
    message_queues: Vec<MessageQueue>,
    topic_route_data: TopicRouteData,
    topic_queue_index: AtomicUsize,
}

impl TopicPublishInfo {
    pub fn get_queue_id_by_broker(&self, broker_name: &str) -> Option<i32> {
        self.topic_route_data
            .queue_datas
            .iter()
            .find(|&queue| queue.broker_name == broker_name)
            .map(|x| x.write_queue_nums)
    }

    pub fn select_message_queue(&mut self) -> &MessageQueue {
        let new_index = self.topic_queue_index.fetch_add(1, Ordering::Relaxed);
        let index = new_index % self.message_queues.len();
        &self.message_queues[index]
    }

    pub fn select_message_queue_exclude_name(&mut self, broker_name: &str) -> &MessageQueue {
        if broker_name.is_empty() {
            return self.select_message_queue();
        }
        let mqs: Vec<_> = self
            .message_queues
            .iter()
            .filter(|&queue| queue.broker_name != broker_name)
            .collect();
        let new_index = self.topic_queue_index.fetch_add(1, Ordering::Relaxed);
        let index = new_index % mqs.len();
        &mqs[index]
    }

    pub fn len(&self) -> usize {
        self.message_queues.len()
    }

    pub fn is_empty(&self) -> bool {
        self.message_queues.is_empty()
    }
}
