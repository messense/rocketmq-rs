use std::collections::HashSet;
use std::fmt;

use serde::Serialize;

use crate::permission::Permission;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ProducerData {
    #[serde(rename = "groupName")]
    pub group_name: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SubscriptionData {
    #[serde(rename = "classFilterMode")]
    pub class_filter_mode: bool,
    pub topic: String,
    #[serde(rename = "subString")]
    pub sub_string: String,
    #[serde(rename = "tagsSet")]
    pub tags_set: HashSet<String>,
    #[serde(rename = "codeSet")]
    pub code_set: HashSet<String>,
    #[serde(rename = "subVersion")]
    pub sub_version: i64,
    #[serde(rename = "expressionType")]
    pub expression_type: String,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct ConsumerData {
    #[serde(rename = "groupName")]
    pub group_name: String,
    #[serde(rename = "consumerType")]
    pub consumer_type: String,
    #[serde(rename = "messageModel")]
    pub message_model: String,
    #[serde(rename = "consumeFromWhere")]
    pub consume_from_where: String,
    #[serde(rename = "subscriptionDataSet")]
    pub subscription_data_set: Vec<SubscriptionData>,
    #[serde(rename = "unitMode")]
    pub unit_mode: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct HeartbeatData {
    #[serde(rename = "clientID")]
    pub client_id: String,
    #[serde(rename = "producerDataSet")]
    pub producer_data_set: Vec<ProducerData>,
    #[serde(rename = "consumerDataSet")]
    pub consumer_data_set: Vec<ConsumerData>,
}

#[derive(Debug, Clone, Copy)]
pub enum TopicFilterType {
    SingleTag,
    MultiTag,
}

impl fmt::Display for TopicFilterType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TopicFilterType::SingleTag => write!(f, "SINGLE_TAG"),
            TopicFilterType::MultiTag => write!(f, "MULTI_TAG"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub(crate) topic_name: String,
    pub(crate) read_queue_nums: u32,
    pub(crate) write_queue_nums: u32,
    pub(crate) permission: Permission,
    pub(crate) topic_filter_type: TopicFilterType,
    pub(crate) topic_sys_flag: i32,
    pub(crate) order: bool,
}

impl TopicConfig {
    pub fn new<S: Into<String>>(topic_name: S) -> Self {
        Self {
            topic_name: topic_name.into(),
            read_queue_nums: 16,
            write_queue_nums: 16,
            permission: Permission::READ | Permission::WRITE,
            topic_filter_type: TopicFilterType::SingleTag,
            topic_sys_flag: 0,
            order: false,
        }
    }
}
