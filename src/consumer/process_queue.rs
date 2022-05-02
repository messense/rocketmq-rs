use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize};

use parking_lot::RwLock;
use time::OffsetDateTime;

#[derive(Debug)]
pub struct ProcessQueue {
    msg_count: AtomicUsize,
    msg_size: AtomicUsize,
    msg_acc_count: AtomicUsize,
    queue_offset_max: i64,
    dropped: AtomicBool,
    last_pull_timestamp: AtomicI64,
    last_consume_timestamp: AtomicI64,
    locked: AtomicBool,
    last_lock_timestamp: AtomicI64,
    consuming: AtomicBool,
}

impl ProcessQueue {
    pub fn new() -> Self {
        let ts = OffsetDateTime::now_utc().unix_timestamp();
        Self {
            msg_count: AtomicUsize::new(0),
            msg_size: AtomicUsize::new(0),
            msg_acc_count: AtomicUsize::new(0),
            queue_offset_max: 0,
            dropped: AtomicBool::new(false),
            last_pull_timestamp: AtomicI64::new(ts),
            last_consume_timestamp: AtomicI64::new(ts),
            locked: AtomicBool::new(false),
            last_lock_timestamp: AtomicI64::new(ts),
            consuming: AtomicBool::new(false),
        }
    }
}

#[cfg(test)]
mod test {
    use super::ProcessQueue;

    #[test]
    fn test_process_queue() {
        let _pq = ProcessQueue::new();
    }
}
