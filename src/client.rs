use std::sync::atomic::{AtomicUsize, Ordering};

use crate::message::MessageQueue;
use crate::route::TopicRouteData;

pub struct Config {
    name_server: String,
    client_ip: String,
    instance_name: String,
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
        let mqs: Vec<_> = self.message_queues
            .iter()
            .filter(|&queue| queue.broker_name != broker_name)
            .collect();
        let new_index = self.topic_queue_index.fetch_add(1, Ordering::Relaxed);
        let index = new_index % mqs.len();
        &mqs[index]
    }
}
