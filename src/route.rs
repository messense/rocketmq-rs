use std::collections::HashMap;

use serde::Deserialize;

use crate::message::MessageQueue;
use crate::permission::Permission;
use crate::Error;

pub(crate) const MASTER_ID: i64 = 0;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct QueueData {
    #[serde(rename = "brokerName")]
    pub broker_name: String,
    #[serde(rename = "readQueueNums")]
    pub read_queue_nums: i32,
    #[serde(rename = "writeQueueNums")]
    pub write_queue_nums: i32,
    pub perm: Permission,
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
        let s = std::str::from_utf8(bytes).unwrap();
        // fixup fastjson mess
        let json = dirty_json::fix(s);
        let data: TopicRouteData = serde_json::from_str(&json)?;
        Ok(data)
    }

    pub fn to_publish_info(&self, topic: &str) -> TopicPublishInfo {
        let mut mqs = Vec::new();
        if !self.order_topic_conf.is_empty() {
            let brokers = self.order_topic_conf.split(';');
            for broker in brokers {
                let mut item = broker.split(':');
                let broker_name = item.next().unwrap();
                let nums: u32 = item.next().unwrap().parse().unwrap();
                for i in 0..nums {
                    mqs.push(MessageQueue {
                        topic: topic.to_string(),
                        broker_name: broker_name.to_string(),
                        queue_id: i,
                    });
                }
            }
            return TopicPublishInfo {
                order_topic: true,
                have_topic_router_info: false,
                route_data: self.clone(),
                message_queues: mqs,
                queue_index: 0,
            };
        }
        for qd in self.queue_datas.iter().rev() {
            if !qd.perm.is_writeable() {
                continue;
            }
            let broker_data = self
                .broker_datas
                .iter()
                .find(|bd| bd.broker_name == qd.broker_name);
            if let Some(broker_data) = broker_data {
                if broker_data
                    .broker_addrs
                    .get(&MASTER_ID)
                    .map(|addr| addr == "")
                    .unwrap_or(true)
                {
                    continue;
                }
                for i in 0..qd.write_queue_nums {
                    mqs.push(MessageQueue {
                        topic: topic.to_string(),
                        broker_name: qd.broker_name.clone(),
                        queue_id: i as u32,
                    });
                }
            }
        }
        TopicPublishInfo {
            order_topic: false,
            have_topic_router_info: false,
            route_data: self.clone(),
            message_queues: mqs,
            queue_index: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicPublishInfo {
    pub order_topic: bool,
    pub have_topic_router_info: bool,
    pub message_queues: Vec<MessageQueue>,
    pub route_data: TopicRouteData,
    pub queue_index: usize,
}
