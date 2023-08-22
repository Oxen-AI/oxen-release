use std::path::PathBuf;

use crate::queues::TaskQueue;

pub struct OxenAppData {
    pub path: PathBuf,
    pub queue: TaskQueue,
}

impl OxenAppData {
    pub fn new(path: &str, queue: TaskQueue) -> OxenAppData {
        OxenAppData {
            path: PathBuf::from(path),
            queue,
        }
    }
}

impl Clone for OxenAppData {
    fn clone(&self) -> Self {
        OxenAppData {
            path: self.path.clone(),
            queue: self.queue.clone(),
        }
    }
}
