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
    topic_queue_index: i32,
}

impl TopicPublishInfo {
    pub fn get_queue_id_by_broker(&self, broker_name: &str) -> Option<i32> {
        self.topic_route_data
            .queue_datas
            .iter()
            .find(|&queue| queue.broker_name == broker_name)
            .map(|x| x.write_queue_nums)
    }

    pub fn select_message_queue_exclude_name(&self, broker_name: &str) -> &MessageQueue {
        unimplemented!()
    }
}
