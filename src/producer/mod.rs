use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::client::{Client, ClientOptions, ClientState};
use crate::error::{ClientError, Error};
use crate::message::{Message, MessageExt, MessageFlag, MessageQueue, Property};
use crate::namesrv::NameServer;
use crate::producer::selector::QueueSelect;
use crate::protocol::{request::SendMessageRequestHeader, RemotingCommand, RequestCode};
use crate::resolver::{HttpResolver, PassthroughResolver, Resolver};
use crate::route::TopicPublishInfo;
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

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum SendStatus {
    Ok = 0,
    FlushDiskTimeout = 1,
    FlushSlaveTimeout = 2,
    SlaveNotAvailable = 3,
    UnknownError = 4,
}

#[derive(Debug, Clone)]
pub struct SendResult {
    pub status: SendStatus,
    pub msg_id: String,
    pub message_queue: MessageQueue,
    pub queue_offset: i64,
    pub transaction_id: Option<String>,
    pub offset_msg_id: String,
    pub region_id: String,
    pub trace_on: bool,
}

#[derive(Debug, Clone)]
pub struct ProducerOptions {
    client_options: ClientOptions,
    selector: QueueSelector,
    resolver: Resolver,
    send_msg_timeout: Duration,
    default_topic_queue_nums: i32,
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

    pub fn set_default_topic_queue_nums(&mut self, queue_nums: i32) -> &mut Self {
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

    pub async fn send(&self, msg: Message) -> Result<SendResult, Error> {
        match self.client.state() {
            ClientState::Created => return Err(Error::Client(ClientError::NotStarted)),
            ClientState::StartFailed => return Err(Error::Client(ClientError::StartFailed)),
            ClientState::Shutdown => return Err(Error::Client(ClientError::Shutdown)),
            _ => {}
        }
        let mut msg = msg;
        let namespace = &self.options.client_options.namespace;
        if !namespace.is_empty() {
            msg.topic = format!("{}%{}", namespace, msg.topic);
        }
        // FIXME: define a ProducerError
        let mq = self.select_message_queue(&msg).await?.unwrap();
        let addr = self
            .client
            .name_server
            .find_broker_addr_by_name(&mq.broker_name)
            .unwrap();
        let cmd = self.build_send_request(&mq, msg.clone());
        let res = self.client.invoke(&addr, cmd).await?;
        Self::process_send_response(&mq.broker_name, res, &[msg])
    }

    fn build_send_request(&self, mq: &MessageQueue, mut msg: Message) -> RemotingCommand {
        msg.set_default_unique_key();
        let mut sys_flag = 0;
        if let Some(tran_msg) = msg.get_property(Property::TRANSACTION_PREPARED) {
            let is_tran_msg: bool = tran_msg.parse().unwrap_or(false);
            if is_tran_msg {
                let tran_prepared: i32 = MessageFlag::TransactionPreparedType.into();
                sys_flag |= tran_prepared;
            }
        }
        let header = SendMessageRequestHeader {
            producer_group: self.options.group_name().to_string(),
            topic: mq.topic.clone(),
            queue_id: mq.queue_id,
            sys_flag,
            born_timestamp: 0,
            flag: msg.flag,
            properties: "".to_string(), // FIXME serialize message properties
            reconsume_times: 0,
            unit_mode: self.options.client_options.unit_mode,
            max_reconsume_times: 0,
            batch: msg.batch,
            default_topic: self.options.create_topic_key.clone(),
            default_topic_queue_nums: self.options.default_topic_queue_nums,
        };
        // FIXME: handle batch message, compress message body
        RemotingCommand::with_header(RequestCode::SendMessage, header, msg.body)
    }

    fn process_send_response(
        broker_name: &str,
        cmd: RemotingCommand,
        msgs: &[Message],
    ) -> Result<SendResult, Error> {
        todo!()
    }

    async fn select_message_queue(&self, msg: &Message) -> Result<Option<MessageQueue>, Error> {
        let topic = msg.topic();
        let info = self.inner.lock().publish_info.get(topic).cloned();
        let info = if info.is_some() {
            info
        } else {
            let (route_data, changed) = self
                .client
                .name_server
                .update_topic_route_info(topic)
                .await?;
            self.client.update_publish_info(topic, route_data, changed);
            self.inner.lock().publish_info.get(topic).cloned()
        };
        let info = if info.is_some() {
            info
        } else {
            let (route_data, changed) = self
                .client
                .name_server
                .update_topic_route_info_with_default(
                    topic,
                    &self.options.create_topic_key,
                    self.options.default_topic_queue_nums,
                )
                .await?;
            self.client.update_publish_info(topic, route_data, changed);
            self.inner.lock().publish_info.get(topic).cloned()
        };
        if let Some(info) = info {
            if info.have_topic_router_info && !info.message_queues.is_empty() {
                return Ok(self.options.selector.select(msg, &info.message_queues));
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use super::Producer;
    use crate::error::{ClientError, Error};
    use crate::message::Message;

    #[tokio::test]
    async fn test_producer_send_error_not_started() {
        let producer = Producer::new().unwrap();
        let msg = Message::new(
            "test".to_string(),
            String::new(),
            String::new(),
            0,
            b"test".to_vec(),
            true,
        );
        let ret = producer.send(msg).await;
        matches!(ret.unwrap_err(), Error::Client(ClientError::NotStarted));
    }
}
