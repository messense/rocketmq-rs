use std::fmt;
use std::sync::Arc;

use crate::message::MessageQueue;
use crate::Error;

#[derive(Debug)]
pub(crate) struct ConsumerInner {}

impl ConsumerInner {
    pub fn rebalance(&self) {
        todo!()
    }
}

#[derive(Debug)]
pub struct Consumer {
    inner: Arc<ConsumerInner>,
}
