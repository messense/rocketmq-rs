use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::client::{Client, ClientOptions};
use crate::namesrv::NameServer;
use crate::resolver::{HttpResolver, Resolver};
use crate::Error;

mod offset_store;
mod push;

use offset_store::{LocalFileOffsetStore, OffsetStorage, RemoteBrokerOffsetStore};
pub use push::PushConsumer;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageModel {
    BroadCasting,
    Clustering,
}

impl fmt::Display for MessageModel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MessageModel::BroadCasting => write!(f, "BroadCasting"),
            MessageModel::Clustering => write!(f, "Clustring"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsumeFrom {
    LastOffset,
    FirstOffset,
    Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExpressionType {
    Sql92,
    Tag,
}

impl fmt::Display for ExpressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExpressionType::Sql92 => write!(f, "SQL92"),
            ExpressionType::Tag => write!(f, "TAG"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    client_options: ClientOptions,
    resolver: Resolver,
    max_reconsume_times: i32,
    consume_timeout: Duration,
    message_model: MessageModel,
    consume_from: ConsumeFrom,
    auto_commit: bool,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            client_options: ClientOptions::default(),
            resolver: Resolver::Http(HttpResolver::new("DEFAULT".to_string())),
            max_reconsume_times: -1,
            consume_timeout: Duration::from_secs(0),
            message_model: MessageModel::Clustering,
            consume_from: ConsumeFrom::LastOffset,
            auto_commit: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsumeResult {
    Success,
    RetryLater,
    Commit,
    Rollback,
    SuspendCurrentQueueAMoment,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsumerReturn {
    Success,
    Exception,
    Null,
    Timeout,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConsumeType {
    Actively,
    Passively,
}

impl fmt::Display for ConsumeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsumeType::Actively => write!(f, "CONSUME_ACTIVELY"),
            ConsumeType::Passively => write!(f, "CONSUME_PASSIVELY"),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ConsumerInner {}

impl ConsumerInner {
    pub fn rebalance(&self) {
        todo!()
    }
}

#[derive(Debug)]
pub struct Consumer {
    inner: Arc<Mutex<ConsumerInner>>,
    options: ConsumerOptions,
    client: Client<Resolver>,
    storage: OffsetStorage,
}

impl Consumer {
    pub fn new() -> Result<Self, Error> {
        Self::with_options(ConsumerOptions::default())
    }

    pub fn with_options(options: ConsumerOptions) -> Result<Self, Error> {
        let client_options = options.client_options.clone();
        let inner = Arc::new(Mutex::new(ConsumerInner {}));
        let name_server =
            NameServer::new(options.resolver.clone(), client_options.credentials.clone())?;
        let client = Client::new(client_options, name_server);
        let consumer_group = &options.client_options.group_name;
        let offset_store = match options.message_model {
            MessageModel::Clustering => OffsetStorage::RemoteBroker(RemoteBrokerOffsetStore::new(
                consumer_group,
                client.clone(),
            )),
            MessageModel::BroadCasting => {
                OffsetStorage::LocalFile(LocalFileOffsetStore::new(consumer_group, &client.id()))
            }
        };
        Ok(Self {
            inner,
            options,
            client,
            storage: offset_store,
        })
    }
}
