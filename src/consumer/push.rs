use super::{Consumer, ConsumerOptions};
use crate::Error;

#[derive(Debug)]
pub struct PushConsumer {
    consumer: Consumer,
}

impl PushConsumer {
    pub fn new() -> Result<Self, Error> {
        Ok(Self {
            consumer: Consumer::new()?,
        })
    }

    pub fn with_options(options: ConsumerOptions) -> Result<Self, Error> {
        Ok(Self {
            consumer: Consumer::with_options(options)?,
        })
    }
}
