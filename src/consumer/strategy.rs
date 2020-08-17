use std::collections::HashSet;

use consistent_hash_ring::RingBuilder;
use tracing::warn;

use crate::message::MessageQueue;

#[derive(Debug, Clone)]
pub enum AllocateStrategy {
    Averagely(AllocateAveragely),
    AveragelyByCircle(AllocateAveragelyByCircle),
    Config(AllocateByConfig),
    MachineRoom(AllocateByMachineRoom),
    ConsistentHash(AllocateConsistentHash),
}

impl AllocateStrategy {
    pub fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        match self {
            AllocateStrategy::Averagely(s) => {
                s.allocate(consumer_group, current_cid, mq_all, cid_all)
            }
            AllocateStrategy::AveragelyByCircle(s) => {
                s.allocate(consumer_group, current_cid, mq_all, cid_all)
            }
            AllocateStrategy::Config(s) => s.allocate(consumer_group, current_cid, mq_all, cid_all),
            AllocateStrategy::MachineRoom(s) => {
                s.allocate(consumer_group, current_cid, mq_all, cid_all)
            }
            AllocateStrategy::ConsistentHash(s) => {
                s.allocate(consumer_group, current_cid, mq_all, cid_all)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocateAveragely;

impl AllocateAveragely {
    pub fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        if current_cid.is_empty() || mq_all.is_empty() || cid_all.is_empty() {
            return Vec::new();
        }
        if let Some(index) = cid_all
            .iter()
            .enumerate()
            .find(|(_index, item)| **item == current_cid)
            .map(|(index, _item)| index)
        {
            let mq_size = mq_all.len();
            let cid_size = cid_all.len();
            let modulo = mq_size % cid_size;
            let avg_size = if mq_size <= cid_size {
                1
            } else {
                if modulo > 0 && index < modulo {
                    mq_size / cid_size + 1
                } else {
                    mq_size / cid_size
                }
            };
            let start_index = if modulo > 0 && index < modulo {
                index * avg_size
            } else {
                index * avg_size + modulo
            };
            let num = std::cmp::min(avg_size, mq_size - start_index);
            let mut mqs = Vec::with_capacity(num);
            for i in 0..num {
                mqs.push(mq_all[(start_index + i) % mq_size].clone());
            }
            mqs
        } else {
            warn!(consumer_group = consumer_group, consumer_id = current_cid, cid_all = ?cid_all, "consumer id not in cid_all");
            Vec::new()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocateAveragelyByCircle;

impl AllocateAveragelyByCircle {
    pub fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        if current_cid.is_empty() || mq_all.is_empty() || cid_all.is_empty() {
            return Vec::new();
        }
        if let Some(index) = cid_all
            .iter()
            .enumerate()
            .find(|(_index, item)| **item == current_cid)
            .map(|(index, _item)| index)
        {
            let mqs: Vec<MessageQueue> = mq_all
                .iter()
                .enumerate()
                .filter_map(|(i, mq)| {
                    if i % cid_all.len() == index {
                        Some(mq.clone())
                    } else {
                        None
                    }
                })
                .collect();
            mqs
        } else {
            warn!(consumer_group = consumer_group, consumer_id = current_cid, cid_all = ?cid_all, "consumer id not in cid_all");
            Vec::new()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocateByConfig(Vec<MessageQueue>);

impl AllocateByConfig {
    pub fn new(mqs: Vec<MessageQueue>) -> Self {
        Self(mqs)
    }
}

impl AllocateByConfig {
    pub fn allocate(
        &self,
        _consumer_group: &str,
        _current_cid: &str,
        _mq_all: &[MessageQueue],
        _cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        self.0.clone()
    }
}

#[derive(Debug, Clone)]
pub struct AllocateByMachineRoom {
    consumer_idcs: HashSet<String>,
}

impl AllocateByMachineRoom {
    pub fn new(consumer_idcs: HashSet<String>) -> Self {
        Self { consumer_idcs }
    }
}

impl AllocateByMachineRoom {
    pub fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        if current_cid.is_empty() || mq_all.is_empty() || cid_all.is_empty() {
            return Vec::new();
        }
        if let Some(index) = cid_all
            .iter()
            .enumerate()
            .find(|(_index, item)| **item == current_cid)
            .map(|(index, _item)| index)
        {
            let premq_all: Vec<&MessageQueue> = mq_all
                .iter()
                .filter(|mq| {
                    let parts: Vec<&str> = mq.broker_name.split('@').collect();
                    if parts.len() == 2 {
                        self.consumer_idcs.contains(parts[0])
                    } else {
                        false
                    }
                })
                .collect();
            let modulo = premq_all.len() / cid_all.len();
            let rem = premq_all.len() % cid_all.len();
            let start_index = modulo * index;
            let end_index = start_index + modulo;
            let mut mqs = mq_all[start_index..end_index].to_vec();
            if rem > index {
                mqs.push(premq_all[index + modulo * cid_all.len()].clone());
            }
            mqs
        } else {
            warn!(consumer_group = consumer_group, consumer_id = current_cid, cid_all = ?cid_all, "consumer id not in cid_all");
            Vec::new()
        }
    }
}

#[derive(Debug, Clone)]
pub struct AllocateConsistentHash {
    virtual_node_count: usize,
}

impl AllocateConsistentHash {
    pub fn new(virtual_node_count: usize) -> Self {
        Self { virtual_node_count }
    }
}

impl AllocateConsistentHash {
    pub fn allocate(
        &self,
        consumer_group: &str,
        current_cid: &str,
        mq_all: &[MessageQueue],
        cid_all: &[&str],
    ) -> Vec<MessageQueue> {
        if current_cid.is_empty() || mq_all.is_empty() || cid_all.is_empty() {
            return Vec::new();
        }
        if cid_all.iter().find(|item| **item == current_cid).is_some() {
            let mut ring = RingBuilder::default()
                .vnodes(self.virtual_node_count)
                .build();
            for cid in cid_all {
                ring.insert(*cid);
            }
            let mut mqs = Vec::new();
            for mq in mq_all {
                if let Some(client_node) = ring.try_get(format!("{:?}", mq)) {
                    if current_cid == *client_node {
                        mqs.push(mq.clone());
                    }
                }
            }
            mqs
        } else {
            warn!(consumer_group = consumer_group, consumer_id = current_cid, cid_all = ?cid_all, "consumer id not in cid_all");
            Vec::new()
        }
    }
}
