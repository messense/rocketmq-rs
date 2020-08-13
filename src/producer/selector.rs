use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::message::{Message, MessageQueue};

pub trait QueueSelect {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> Option<MessageQueue>;
}

#[derive(Debug, Clone)]
pub enum QueueSelector {
    Manual(ManualQueueSelector),
    Random(RandomQueueSelector),
    RoundRobin(RoundRobinQueueSelector),
    Hash(HashQueueSelector),
}

impl QueueSelect for QueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> Option<MessageQueue> {
        match self {
            QueueSelector::Manual(inner) => inner.select(msg, mqs),
            QueueSelector::Random(inner) => inner.select(msg, mqs),
            QueueSelector::RoundRobin(inner) => inner.select(msg, mqs),
            QueueSelector::Hash(inner) => inner.select(msg, mqs),
        }
    }
}

impl Default for QueueSelector {
    fn default() -> Self {
        Self::RoundRobin(RoundRobinQueueSelector::new())
    }
}

#[derive(Debug, Clone)]
pub struct ManualQueueSelector;

impl QueueSelect for ManualQueueSelector {
    fn select(&self, msg: &Message, _mqs: &[MessageQueue]) -> Option<MessageQueue> {
        let mq = msg.queue.as_ref();
        mq.cloned()
    }
}

#[derive(Debug, Clone)]
pub struct RandomQueueSelector;

impl QueueSelect for RandomQueueSelector {
    fn select(&self, _msg: &Message, mqs: &[MessageQueue]) -> Option<MessageQueue> {
        use rand::prelude::*;

        let mut rng = thread_rng();
        mqs.iter().choose(&mut rng).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct RoundRobinQueueSelector {
    // topic -> index
    indexer: Arc<Mutex<HashMap<String, usize>>>,
}

impl RoundRobinQueueSelector {
    pub fn new() -> Self {
        Self {
            indexer: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl QueueSelect for RoundRobinQueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> Option<MessageQueue> {
        let topic = msg.topic();
        let mut indexer = self.indexer.lock();
        let i = indexer
            .entry(topic.to_string())
            .and_modify(|e| *e = e.wrapping_add(1))
            .or_insert(1);
        let index = *i % mqs.len();
        mqs.get(index).cloned()
    }
}

#[derive(Debug, Clone)]
pub struct HashQueueSelector {
    random: RandomQueueSelector,
}

impl HashQueueSelector {
    pub fn new() -> Self {
        Self {
            random: RandomQueueSelector,
        }
    }
}

impl QueueSelect for HashQueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> Option<MessageQueue> {
        if let Some(key) = msg.sharding_key() {
            let mut hasher = fnv::FnvHasher::default();
            hasher.write(key.as_bytes());
            let index = hasher.finish() as usize % mqs.len();
            mqs.get(index).cloned()
        } else {
            self.random.select(msg, mqs)
        }
    }
}
