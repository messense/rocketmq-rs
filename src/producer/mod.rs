use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::client::{Client, ClientOptions};
use crate::message::MessageExt;
use crate::namesrv::NameServer;
use crate::resolver::{HttpResolver, NsResolver, PassthroughResolver};
use crate::Error;
use selector::{QueueSelector, RoundRobinQueueSelector};

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

pub struct ProducerOptions<R: NsResolver = HttpResolver> {
    selector: Box<dyn QueueSelector>,
    send_msg_timeout: Duration,
    default_topic_queue_nums: usize,
    create_topic_key: String,
    resolver: R,
}

impl fmt::Debug for ProducerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProducerOptions")
            .field("send_msg_timeout", &self.send_msg_timeout)
            .field("default_queue_nums", &self.default_topic_queue_nums)
            .field("create_topic_key", &self.create_topic_key)
            .field("resolver", &self.resolver.description())
            .finish()
    }
}

impl Default for ProducerOptions {
    fn default() -> ProducerOptions {
        Self {
            selector: Box::new(RoundRobinQueueSelector::new()),
            send_msg_timeout: Duration::from_secs(3),
            default_topic_queue_nums: 4,
            create_topic_key: "TBW102".to_string(),
            resolver: HttpResolver::new("DEFAULT".to_string()),
        }
    }
}

impl ProducerOptions {
    pub fn new() -> Self {
        ProducerOptions::default()
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

    pub fn set_name_server_domain(&mut self, url: &str) -> &mut Self {
        self.resolver = HttpResolver::with_domain("DEFAULT".to_string(), url.to_string());
        self
    }
}

impl<R: NsResolver> ProducerOptions<R> {
    pub fn set_resolver(&mut self, resolver: R) -> &mut Self {
        self.resolver = resolver;
        self
    }
}

impl ProducerOptions<PassthroughResolver<HttpResolver>> {
    pub fn set_name_server(&mut self, addrs: Vec<String>) -> &mut Self {
        self.resolver = PassthroughResolver::new(addrs, HttpResolver::new("DEFAULT".to_string()));
        self
    }
}

pub(crate) struct ProducerInner {
    group: String,
    options: ProducerOptions,
    client: Client<HttpResolver>,
}

impl fmt::Debug for ProducerInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("group", &self.group)
            .field("options", &self.options)
            .finish()
    }
}

impl ProducerInner {
    fn new(group: &str) -> Result<Self, Error> {
        Self::with_options(group, ProducerOptions::new())
    }

    fn with_options(group: &str, options: ProducerOptions) -> Result<Self, Error> {
        let client_options = ClientOptions::new(group);
        let name_server = NameServer::new(options.resolver.clone())?;
        Ok(Self {
            group: group.to_string(),
            options,
            client: Client::new(client_options, name_server),
        })
    }
}

/// RocketMQ producer
pub struct Producer {
    inner: Arc<ProducerInner>,
}

impl fmt::Debug for Producer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Producer")
            .field("group", &self.inner.group)
            .field("options", &self.inner.options)
            .finish()
    }
}

impl Producer {
    pub fn start(&self) {
        self.inner
            .client
            .register_producer(&self.inner.group, Arc::clone(&self.inner));
        self.inner.client.start();
    }

    pub fn shutdown(&self) {
        self.inner.client.unregister_producer(&self.inner.group);
        self.inner.client.shutdown();
    }
}
