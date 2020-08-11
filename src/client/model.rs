use std::collections::HashSet;

use serde::Serialize;

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
