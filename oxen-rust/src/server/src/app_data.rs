use liboxen::core::v0_10_0::index::CommitDirEntryReader;

use std::path::PathBuf;

use crate::queues::TaskQueue;
use lru::LruCache;
use std::sync::{Arc, RwLock};

pub struct OxenAppData {
    pub path: PathBuf,
    pub queue: TaskQueue,
    // CommitEntryReaderLeastRecentlyUsed
    pub cder_lru: Arc<RwLock<LruCache<String, CommitDirEntryReader>>>,
}

impl OxenAppData {
    pub fn new(path: PathBuf, queue: TaskQueue) -> OxenAppData {
        let cder_lru: Arc<RwLock<LruCache<String, CommitDirEntryReader>>> = Arc::new(RwLock::new(
            LruCache::new(std::num::NonZeroUsize::new(128).unwrap()),
        ));
        OxenAppData {
            path,
            queue,
            cder_lru,
        }
    }
}

impl Clone for OxenAppData {
    fn clone(&self) -> Self {
        OxenAppData {
            path: self.path.clone(),
            queue: self.queue.clone(),
            cder_lru: self.cder_lru.clone(),
        }
    }
}
