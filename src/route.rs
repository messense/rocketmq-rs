use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::prelude::*;
use serde::Serialize;

use crate::message::MessageQueue;

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
