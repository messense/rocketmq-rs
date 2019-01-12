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