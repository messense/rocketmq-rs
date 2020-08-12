use std::sync::Arc;

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
