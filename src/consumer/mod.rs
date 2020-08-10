use crate::client::InnerConsumer;
use crate::message::MessageQueue;
use crate::Error;

#[derive(Debug)]
pub struct Consumer {}

impl InnerConsumer for Consumer {
    fn persist_consumer_offset(&self) -> Result<(), Error> {
        unimplemented!()
    }

    fn update_topic_subscribe_info(&self, topic: &str, mqs: &[MessageQueue]) {
        unimplemented!()
    }

    fn is_subscribe_topic_need_update(&self, topic: &str) -> bool {
        unimplemented!()
    }

    fn rebalance(&self) {
        unimplemented!()
    }

    fn get_c_type(&self) -> String {
        unimplemented!()
    }

    fn get_model(&self) -> String {
        unimplemented!()
    }

    fn get_where(&self) -> String {
        unimplemented!()
    }
}
