use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tracing::{error, warn};

use crate::client::Client;
use crate::message::MessageQueue;
use crate::resolver::Resolver;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReadType {
    Memory,
    Store,
    MemoryThenStore,
}

#[derive(Debug)]
pub enum OffsetStorage {
    LocalFile(LocalFileOffsetStore),
    RemoteBroker(RemoteBrokerOffsetStore),
}

#[async_trait]
pub trait OffsetStore {
    async fn persist(&self, mqs: &[MessageQueue]);
    fn remove(&self, mq: &MessageQueue);
    async fn read(&self, mq: &MessageQueue, read_type: ReadType) -> i64;
    fn update(&self, mq: &MessageQueue, offset: i64, increase_only: bool);
}

#[derive(Debug, Deserialize, Serialize)]
struct OffsetTableWrapper {
    #[serde(rename = "offsetTable")]
    offset_table: HashMap<MessageQueue, i64>,
}

#[derive(Debug)]
pub struct LocalFileOffsetStore {
    group: String,
    path: PathBuf,
    offset_table: Mutex<HashMap<MessageQueue, i64>>,
}

impl LocalFileOffsetStore {
    pub fn new(client_id: &str, group: &str) -> Self {
        let store_path = env::var("rocketmq.client.localOffsetStoreDir")
            .unwrap_or_else(|_| env::var("HOME").unwrap() + ".rocketmq_client_rust");
        Self {
            group: group.to_string(),
            path: PathBuf::from(store_path)
                .join(client_id)
                .join(group)
                .join("offset.json"),
            offset_table: Mutex::new(HashMap::new()),
        }
    }

    async fn load(&self) {
        let data = match tokio::fs::read(&self.path).await {
            Ok(data) => data,
            Err(err) => {
                warn!(
                    "read from local store error, try to use bak file: {:?}",
                    err
                );
                let mut bak_path = self.path.clone();
                bak_path.set_file_name("offset.json.bak");
                match tokio::fs::read(&bak_path).await {
                    Ok(data) => data,
                    Err(err) => {
                        warn!("read from local store bak file error: {:?}", err);
                        return;
                    }
                }
            }
        };
        match serde_json::from_slice::<OffsetTableWrapper>(&data) {
            Ok(wrapper) => {
                *self.offset_table.lock() = wrapper.offset_table;
            }
            Err(err) => {
                warn!("deserialize local offset error: {:?}", err);
                return;
            }
        }
    }

    fn read_from_memory(&self, mq: &MessageQueue) -> i64 {
        self.offset_table.lock().get(mq).cloned().unwrap_or(-1)
    }
}

#[async_trait]
impl OffsetStore for LocalFileOffsetStore {
    async fn persist(&self, mqs: &[MessageQueue]) {
        if mqs.is_empty() {
            return;
        }
        let wrapper = OffsetTableWrapper {
            offset_table: self.offset_table.lock().clone(),
        };
        match serde_json::to_vec(&wrapper) {
            Ok(data) => {
                if let Err(err) = tokio::fs::write(&self.path, data).await {
                    error!(
                        "persist offset to {} failed: {:?}",
                        self.path.display(),
                        err
                    );
                }
            }
            Err(err) => error!(
                "persist offset to {} failed, serialize to json failed: {:?}",
                self.path.display(),
                err
            ),
        }
    }
    fn remove(&self, _mq: &MessageQueue) {
        // do nothing
    }

    async fn read(&self, mq: &MessageQueue, read_type: ReadType) -> i64 {
        match read_type {
            ReadType::Memory | ReadType::MemoryThenStore => self.read_from_memory(mq),
            ReadType::Store => {
                self.load().await;
                self.read_from_memory(mq)
            }
        }
    }

    fn update(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        self.offset_table
            .lock()
            .entry(mq.clone())
            .and_modify(|local_offset| {
                if increase_only {
                    if *local_offset < offset {
                        *local_offset = offset;
                    }
                } else {
                    *local_offset = offset;
                }
            })
            .or_insert(offset);
    }
}

#[derive(Debug)]
pub struct RemoteBrokerOffsetStore {
    group: String,
    client: Client<Resolver>,
    offset_table: Mutex<HashMap<MessageQueue, i64>>,
}

impl RemoteBrokerOffsetStore {
    pub fn new(group: &str, client: Client<Resolver>) -> Self {
        Self {
            group: group.to_string(),
            client,
            offset_table: Mutex::new(HashMap::new()),
        }
    }

    fn read_from_memory(&self, mq: &MessageQueue) -> i64 {
        self.offset_table.lock().get(mq).cloned().unwrap_or(-1)
    }

    async fn read_from_broker(&self, mq: MessageQueue) -> i64 {
        todo!()
    }
}

#[async_trait]
impl OffsetStore for RemoteBrokerOffsetStore {
    async fn persist(&self, mqs: &[MessageQueue]) {
        if mqs.is_empty() {
            return;
        }
        todo!()
    }

    fn remove(&self, mq: &MessageQueue) {
        self.offset_table.lock().remove(mq);
        warn!(consumer_group = %self.group, message_queue = ?mq, "delete message queue from offset table");
    }

    async fn read(&self, mq: &MessageQueue, read_type: ReadType) -> i64 {
        match read_type {
            ReadType::Memory => self.read_from_memory(mq),
            ReadType::MemoryThenStore => {
                let offset = self.read_from_memory(mq);
                if offset != -1 {
                    return offset;
                }
                self.read_from_broker(mq.clone()).await
            }
            ReadType::Store => self.read_from_broker(mq.clone()).await,
        }
    }

    fn update(&self, mq: &MessageQueue, offset: i64, increase_only: bool) {
        self.offset_table
            .lock()
            .entry(mq.clone())
            .and_modify(|local_offset| {
                if increase_only {
                    if *local_offset < offset {
                        *local_offset = offset;
                    }
                } else {
                    *local_offset = offset;
                }
            })
            .or_insert(offset);
    }
}
