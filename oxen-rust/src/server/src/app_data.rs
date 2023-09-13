use liboxen::core::index::CommitDirEntryReader;

use std::path::PathBuf;

use crate::queues::TaskQueue;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};

pub struct OxenAppData {
    pub path: PathBuf,
    pub queue: TaskQueue,
    pub cder_lru: Arc<RwLock<LruCache<String, CommitDirEntryReader>>>, // CommitEntryReaderLeastRecentlyUsed,
}

impl OxenAppData {
    pub fn new(
        path: &str,
        queue: TaskQueue,
        cder_lru: Arc<RwLock<LruCache<String, CommitDirEntryReader>>>,
    ) -> OxenAppData {
        OxenAppData {
            path: PathBuf::from(path),
            queue,
            cder_lru,
        }
    }
}

impl Clone for OxenAppData {
    fn clone(&self) -> Self {
        let cder_lru: Arc<RwLock<LruCache<String, CommitDirEntryReader>>> =
            Arc::new(RwLock::new(LruCache::new(NonZeroUsize::new(2).unwrap())));
        OxenAppData {
            path: self.path.clone(),
            queue: self.queue.clone(),
            cder_lru,
        }
    }
}
