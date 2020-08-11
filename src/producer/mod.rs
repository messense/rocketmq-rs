use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::client::{Client, ClientOptions};
use crate::message::{Message, MessageExt, MessageQueue};
use crate::namesrv::NameServer;
use crate::resolver::{HttpResolver, PassthroughResolver, Resolver};
use crate::route::TopicPublishInfo;
use crate::Error;
use selector::QueueSelector;

pub mod selector;

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum PullStatus {
    Found = 0,
    NoNewMsg = 1,
    NoMsgMatched = 2,
    OffsetIllegal = 3,
    BrokerTimeout = 4,
}

#[derive(Debug, Clone)]
pub struct PullResult {
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
    pub status: PullStatus,
    pub suggest_which_broker_id: i64,
    pub message_exts: Vec<MessageExt>,
    pub body: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ProducerOptions {
    client_options: ClientOptions,
    selector: QueueSelector,
    resolver: Resolver,
    send_msg_timeout: Duration,
    default_topic_queue_nums: usize,
    create_topic_key: String,
    compress_msg_body_over_how_much: usize,
    max_message_size: usize,
    max_retries: usize,
}

impl Default for ProducerOptions {
    fn default() -> ProducerOptions {
        Self {
            client_options: ClientOptions::default(),
            selector: QueueSelector::default(),
            resolver: Resolver::Http(HttpResolver::new("DEFAULT".to_string())),
            send_msg_timeout: Duration::from_secs(3),
            default_topic_queue_nums: 4,
            create_topic_key: "TBW102".to_string(),
            compress_msg_body_over_how_much: 4 * 1024, // 4K
            max_message_size: 4 * 1024 * 1024,         // 4M
            max_retries: 2,
        }
    }
}

impl ProducerOptions {
    pub fn new() -> Self {
        ProducerOptions::default()
    }

    pub fn with_client_options(client_options: ClientOptions) -> Self {
        Self {
            client_options,
            ..Default::default()
        }
    }

    pub fn group_name(&self) -> &str {
        &self.client_options.group_name
    }

    pub fn set_send_msg_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.send_msg_timeout = timeout;
        self
    }

    pub fn set_default_topic_queue_nums(&mut self, queue_nums: usize) -> &mut Self {
        self.default_topic_queue_nums = queue_nums;
        self
    }

    pub fn set_create_topic_key(&mut self, key: &str) -> &mut Self {
        self.create_topic_key = key.to_string();
        self
    }

    pub fn set_resolver(&mut self, resolver: Resolver) -> &mut Self {
        self.resolver = resolver;
        self
    }

    pub fn set_name_server(&mut self, addrs: Vec<String>) -> &mut Self {
        self.resolver = Resolver::PassthroughHttp(PassthroughResolver::new(
            addrs,
            HttpResolver::new("DEFAULT".to_string()),
        ));
        self
    }

    pub fn set_name_server_domain(&mut self, url: &str) -> &mut Self {
        self.resolver = Resolver::Http(HttpResolver::with_domain(
            "DEFAULT".to_string(),
            url.to_string(),
        ));
        self
    }
}

#[derive(Debug)]
pub(crate) struct ProducerInner {
    publish_info: HashMap<String, TopicPublishInfo>,
}

impl ProducerInner {
    fn new() -> Self {
        Self {
            publish_info: HashMap::new(),
        }
    }

    fn select_message_queue(&self, msg: &Message) -> MessageQueue {
        todo!()
    }

    pub(crate) fn publish_topic_list(&self) -> Vec<String> {
        self.publish_info.keys().cloned().collect()
    }

    pub(crate) fn update_topic_publish_info(&mut self, topic: &str, info: TopicPublishInfo) {
        if !topic.is_empty() {
            self.publish_info.insert(topic.to_string(), info);
        }
    }

    pub(crate) fn is_publish_topic_need_update(&self, topic: &str) -> bool {
        self.publish_info
            .get(topic)
            .map(|info| info.message_queues.is_empty())
            .unwrap_or(true)
    }

    pub(crate) fn is_unit_mode(&self) -> bool {
        false
    }
}

/// RocketMQ producer
#[derive(Debug)]
pub struct Producer {
    inner: Arc<Mutex<ProducerInner>>,
    options: ProducerOptions,
    client: Client<Resolver>,
}

impl Producer {
    pub fn new() -> Result<Self, Error> {
        Self::with_options(ProducerOptions::default())
    }

    pub fn with_options(options: ProducerOptions) -> Result<Self, Error> {
        let client_options = options.client_options.clone();
        let name_server =
            NameServer::new(options.resolver.clone(), client_options.credentials.clone())?;
        Ok(Self {
            inner: Arc::new(Mutex::new(ProducerInner::new())),
            options,
            client: Client::new(client_options, name_server),
        })
    }

    pub fn start(&self) {
        self.client
            .register_producer(&self.options.group_name(), Arc::clone(&self.inner));
        self.client.start();
    }

    pub fn shutdown(&self) {
        self.client.unregister_producer(&self.options.group_name());
        self.client.shutdown();
    }
}
