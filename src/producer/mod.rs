use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::{self, Write};
use std::sync::Arc;
use std::time::Duration;

use flate2::write::ZlibEncoder;
use flate2::Compression;
use parking_lot::Mutex;
use time::OffsetDateTime;

use crate::client::{Client, ClientOptions, ClientState};
use crate::error::{ClientError, Error};
use crate::message::{Message, MessageQueue, MessageSysFlag, Property};
use crate::namesrv::NameServer;
use crate::producer::selector::QueueSelect;
use crate::protocol::{
    request::{SendMessageRequestHeader, SendMessageRequestV2Header},
    RemotingCommand, RequestCode, ResponseCode,
};
use crate::resolver::{HttpResolver, PassthroughResolver, Resolver};
use crate::route::TopicPublishInfo;
use selector::QueueSelector;

/// Message queue selector
pub mod selector;

/// Message send status
#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(i32)]
pub enum SendStatus {
    Ok = 0,
    FlushDiskTimeout = 1,
    FlushSlaveTimeout = 2,
    SlaveNotAvailable = 3,
    UnknownError = 4,
}

/// Message send result
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

/// RocketMQ producer options
#[derive(Debug, Clone)]
pub struct ProducerOptions {
    client_options: ClientOptions,
    selector: QueueSelector,
    resolver: Resolver,
    send_msg_timeout: Duration,
    default_topic_queue_nums: i32,
    create_topic_key: String,
    compress_msg_body_over_how_much: usize,
    compress_level: u32,
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
            compress_level: 5,
            max_message_size: 4 * 1024 * 1024, // 4M
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

    fn check_state(&self) -> Result<(), Error> {
        match self.client.state() {
            ClientState::Created => Err(Error::Client(ClientError::NotStarted)),
            ClientState::StartFailed => Err(Error::Client(ClientError::StartFailed)),
            ClientState::Shutdown => Err(Error::Client(ClientError::Shutdown)),
            _ => Ok(()),
        }
    }

    pub async fn send(&self, msg: Message) -> Result<SendResult, Error> {
        self.check_state()?;
        let mut msg = msg;
        let namespace = &self.options.client_options.namespace;
        if !namespace.is_empty() {
            msg.topic = format!("{}%{}", namespace, msg.topic);
        }
        let mq = self
            .select_message_queue(&msg)
            .await?
            .ok_or(Error::EmptyRouteData)?;
        let addr = self
            .client
            .name_server
            .find_broker_addr_by_name(&mq.broker_name)
            .ok_or(Error::EmptyRouteData)?;
        let cmd = self.build_send_request(&mq, &mut msg)?;
        let res = tokio::time::timeout(
            self.options.send_msg_timeout.clone(),
            self.client.invoke(&addr, cmd),
        )
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::TimedOut, e))??;
        Self::process_send_response(&mq.broker_name, res, &[msg])
    }

    pub async fn send_batch(&self, msgs: &[Message]) -> Result<SendResult, Error> {
        let msg = Message::encode_batch(msgs)?;
        Ok(self.send(msg).await?)
    }

    pub async fn send_oneway(&self, msg: Message) -> Result<(), Error> {
        self.check_state()?;
        let mut msg = msg;
        let namespace = &self.options.client_options.namespace;
        if !namespace.is_empty() {
            msg.topic = format!("{}%{}", namespace, msg.topic);
        }
        let mq = self
            .select_message_queue(&msg)
            .await?
            .ok_or(Error::EmptyRouteData)?;
        let addr = self
            .client
            .name_server
            .find_broker_addr_by_name(&mq.broker_name)
            .ok_or(Error::EmptyRouteData)?;
        let cmd = self.build_send_request(&mq, &mut msg)?;
        Ok(self.client.invoke_oneway(&addr, cmd).await?)
    }

    pub async fn send_batch_oneway(&self, msgs: &[Message]) -> Result<(), Error> {
        let msg = Message::encode_batch(msgs)?;
        Ok(self.send_oneway(msg).await?)
    }

    fn build_send_request(
        &self,
        mq: &MessageQueue,
        msg: &mut Message,
    ) -> Result<RemotingCommand, Error> {
        msg.set_default_unique_key();
        let mut sys_flag = 0;
        if let Some(tran_msg) = msg.get_property(Property::TRANSACTION_PREPARED) {
            let is_tran_msg: bool = tran_msg.parse().unwrap_or(false);
            if is_tran_msg {
                let tran_prepared: i32 = MessageSysFlag::TransactionPreparedType.into();
                sys_flag |= tran_prepared;
            }
        }
        let body = if !msg.batch {
            let compressed_flag: i32 = MessageSysFlag::Compressed.into();
            if msg.sys_flag & compressed_flag == compressed_flag {
                // Already compressed
                msg.body.clone()
            } else {
                if msg.body.len() >= self.options.compress_msg_body_over_how_much {
                    let mut encoder =
                        ZlibEncoder::new(Vec::new(), Compression::new(self.options.compress_level));
                    encoder.write_all(&msg.body)?;
                    let compressed = encoder.finish()?;
                    msg.sys_flag |= compressed_flag;
                    compressed
                } else {
                    msg.body.clone()
                }
            }
        } else {
            msg.body.clone()
        };
        let cmd = if msg.batch {
            let header = SendMessageRequestV2Header {
                producer_group: self.options.group_name().to_string(),
                topic: mq.topic.clone(),
                queue_id: mq.queue_id,
                sys_flag,
                born_timestamp: (OffsetDateTime::now_utc() - OffsetDateTime::UNIX_EPOCH)
                    .whole_milliseconds() as i64,
                flag: msg.flag,
                properties: msg.dump_properties(),
                reconsume_times: 0,
                unit_mode: self.options.client_options.unit_mode,
                max_reconsume_times: 0,
                batch: msg.batch,
                default_topic: self.options.create_topic_key.clone(),
                default_topic_queue_nums: self.options.default_topic_queue_nums,
            };
            RemotingCommand::with_header(RequestCode::SendMessageV2, header, body)
        } else {
            let header = SendMessageRequestHeader {
                producer_group: self.options.group_name().to_string(),
                topic: mq.topic.clone(),
                queue_id: mq.queue_id,
                sys_flag,
                born_timestamp: (OffsetDateTime::now_utc() - OffsetDateTime::UNIX_EPOCH)
                    .whole_milliseconds() as i64,
                flag: msg.flag,
                properties: msg.dump_properties(),
                reconsume_times: 0,
                unit_mode: self.options.client_options.unit_mode,
                max_reconsume_times: 0,
                batch: msg.batch,
                default_topic: self.options.create_topic_key.clone(),
                default_topic_queue_nums: self.options.default_topic_queue_nums,
            };
            RemotingCommand::with_header(RequestCode::SendMessage, header, body)
        };
        Ok(cmd)
    }

    fn process_send_response(
        broker_name: &str,
        cmd: RemotingCommand,
        msgs: &[Message],
    ) -> Result<SendResult, Error> {
        let status = match ResponseCode::try_from(cmd.code()).unwrap_or(ResponseCode::SystemError) {
            ResponseCode::FlushDiskTimeout => SendStatus::FlushDiskTimeout,
            ResponseCode::FlushSlaveTimeout => SendStatus::FlushDiskTimeout,
            ResponseCode::SlaveNotAvailable => SendStatus::SlaveNotAvailable,
            ResponseCode::Success => SendStatus::Ok,
            _ => {
                return Err(Error::ResponseError {
                    code: cmd.code(),
                    message: cmd.header.remark,
                })
            }
        };
        let uniq_msg_id = msgs
            .iter()
            .filter_map(|msg| msg.unique_key())
            .collect::<Vec<&str>>()
            .join(",");
        let region_id = cmd
            .header
            .ext_fields
            .get(Property::MSG_REGION)
            .cloned()
            .unwrap_or_else(|| "DefaultRegion".to_string());
        let trace_on = cmd
            .header
            .ext_fields
            .get(Property::TRACE_SWITCH)
            .map(|prop| !prop.is_empty() && prop != "false")
            .unwrap_or(false);
        let queue_id: u32 = cmd.header.ext_fields["queueId"].parse().unwrap();
        let queue_offset: i64 = cmd.header.ext_fields["queueOffset"].parse().unwrap();
        let result = SendResult {
            status,
            msg_id: uniq_msg_id,
            message_queue: MessageQueue {
                topic: msgs[0].topic.clone(),
                broker_name: broker_name.to_string(),
                queue_id,
            },
            queue_offset,
            transaction_id: cmd.header.ext_fields.get("transactionId").cloned(),
            offset_msg_id: cmd.header.ext_fields["msgId"].clone(),
            region_id,
            trace_on,
        };
        Ok(result)
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

impl Drop for Producer {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod test {
    use super::{Producer, ProducerOptions, SendStatus};
    use crate::error::{ClientError, Error};
    use crate::message::{Message, MessageQueue};

    #[tokio::test]
    async fn test_producer_send_error_not_started() {
        let producer = Producer::new().unwrap();
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            b"test".to_vec(),
            true,
        );
        let ret = producer.send(msg).await;
        matches!(ret.unwrap_err(), Error::Client(ClientError::NotStarted));
    }

    #[tokio::test]
    async fn test_producer_send_message() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            b"test".to_vec(),
            false,
        );
        let ret = producer.send(msg).await.unwrap();
        assert_eq!(ret.status, SendStatus::Ok);
    }

    #[tokio::test]
    async fn test_producer_send_message_compressed() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let body = b"test-compressed".to_vec().repeat(1024);
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            body,
            false,
        );
        let ret = producer.send(msg).await.unwrap();
        assert_eq!(ret.status, SendStatus::Ok);
    }

    #[tokio::test]
    async fn test_producer_send_batch_message_empty() {
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let ret = producer.send_batch(&[]).await;
        assert!(matches!(ret.unwrap_err(), Error::EmptyBatchMessage));
    }

    #[tokio::test]
    async fn test_producer_send_batch_message_one_msg() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            b"test-batch-1".to_vec(),
            false,
        );
        let ret = producer.send_batch(&[msg]).await.unwrap();
        assert_eq!(ret.status, SendStatus::Ok);
    }

    #[tokio::test]
    async fn test_producer_send_batch_message_multi_msg() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msgs = [
            Message::new(
                "SELF_TEST_TOPIC".to_string(),
                String::new(),
                String::new(),
                0,
                b"test-batch-1".to_vec(),
                false,
            ),
            Message::new(
                "SELF_TEST_TOPIC".to_string(),
                String::new(),
                String::new(),
                0,
                b"test-batch-2".to_vec(),
                false,
            ),
        ];
        let ret = producer.send_batch(&msgs).await.unwrap();
        assert_eq!(ret.status, SendStatus::Ok);
    }

    #[tokio::test]
    async fn test_producer_send_message_oneway() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            b"test_oneway".to_vec(),
            false,
        );
        producer.send_oneway(msg).await.unwrap();
    }

    #[tokio::test]
    async fn test_producer_send_batch_oneway_message_empty() {
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let ret = producer.send_batch_oneway(&[]).await;
        assert!(matches!(ret.unwrap_err(), Error::EmptyBatchMessage));
    }

    #[tokio::test]
    async fn test_producer_send_batch_oneway_message_one_msg() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msg = Message::new(
            "SELF_TEST_TOPIC".to_string(),
            String::new(),
            String::new(),
            0,
            b"test-batch-oneway-1".to_vec(),
            false,
        );
        producer.send_batch_oneway(&[msg]).await.unwrap();
    }

    #[tokio::test]
    async fn test_producer_send_batch_oneway_message_multi_msg() {
        // tracing_subscriber::fmt::init();
        let mut options = ProducerOptions::default();
        options.set_name_server(vec!["localhost:9876".to_string()]);
        let producer = Producer::with_options(options).unwrap();
        producer.start();
        let msgs = [
            Message::new(
                "SELF_TEST_TOPIC".to_string(),
                String::new(),
                String::new(),
                0,
                b"test-batch-oneway-1".to_vec(),
                false,
            ),
            Message::new(
                "SELF_TEST_TOPIC".to_string(),
                String::new(),
                String::new(),
                0,
                b"test-batch-oneway-2".to_vec(),
                false,
            ),
        ];
        producer.send_batch_oneway(&msgs).await.unwrap();
    }

    #[test]
    fn test_producer_build_send_request_no_compression() {
        let producer = Producer::new().unwrap();
        let body = b"test".to_vec();
        let mut msg = Message::new(
            "test".to_string(),
            String::new(),
            String::new(),
            0,
            body.clone(),
            true,
        );
        let mq = MessageQueue {
            topic: "test".to_string(),
            broker_name: "DefaultCluster".to_string(),
            queue_id: 0,
        };
        let cmd = producer.build_send_request(&mq, &mut msg).unwrap();
        assert_eq!(body, cmd.body);
    }

    #[test]
    fn test_producer_build_send_request_compressed() {
        let producer = Producer::new().unwrap();
        let body = b"test".to_vec().repeat(1024);
        let mut msg = Message::new(
            "test".to_string(),
            String::new(),
            String::new(),
            0,
            body.clone(),
            true,
        );
        let mq = MessageQueue {
            topic: "test".to_string(),
            broker_name: "DefaultCluster".to_string(),
            queue_id: 0,
        };
        let cmd = producer.build_send_request(&mq, &mut msg).unwrap();
        assert_ne!(body, cmd.body);
    }
}
