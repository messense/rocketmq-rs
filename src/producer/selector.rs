use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::{Arc, Mutex};

use crate::message::{Message, MessageQueue};

pub trait QueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> MessageQueue;
}

#[derive(Debug, Clone)]
pub struct ManualQueueSelector;

impl QueueSelector for ManualQueueSelector {
    fn select(&self, msg: &Message, _mqs: &[MessageQueue]) -> MessageQueue {
        let mq = msg.queue.as_ref().unwrap();
        mq.clone()
    }
}

#[derive(Debug, Clone)]
pub struct RandomQueueSelector;

impl QueueSelector for RandomQueueSelector {
    fn select(&self, _msg: &Message, mqs: &[MessageQueue]) -> MessageQueue {
        use rand::prelude::*;

        let mut rng = thread_rng();
        mqs.iter().choose(&mut rng).unwrap().clone()
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

impl QueueSelector for RoundRobinQueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> MessageQueue {
        let topic = msg.topic();
        let mut indexer = self.indexer.lock().unwrap();
        let i = indexer
            .entry(topic.to_string())
            .and_modify(|e| *e = e.wrapping_add(1))
            .or_insert(0);
        let index = *i % mqs.len();
        mqs[index].clone()
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

impl QueueSelector for HashQueueSelector {
    fn select(&self, msg: &Message, mqs: &[MessageQueue]) -> MessageQueue {
        if let Some(key) = msg.sharding_key() {
            let mut hasher = fnv::FnvHasher::default();
            hasher.write(key.as_bytes());
            let index = hasher.finish() as usize % mqs.len();
            mqs[index].clone()
        } else {
            self.random.select(msg, mqs)
        }
    }
}
