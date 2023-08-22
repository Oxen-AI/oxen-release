use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};

use liboxen::constants::COMMIT_QUEUE_NAME;
use redis::Connection;

use crate::tasks::post_push_complete::PostPushComplete;
use crate::tasks::Task;

pub trait TaskQueue: Send {
    fn push(&mut self, task: Task);
    fn pop(&mut self) -> Option<Task>;
}

pub struct RedisTaskQueue {
    pub conn: redis::Connection,
}

impl TaskQueue for RedisTaskQueue {
    fn push(&mut self, task: Task) {
        let data: Vec<u8>;
        match task {
            Task::PostPushComplete(task) => {
                data = bincode::serialize(&task).unwrap();
            }
        }

        let _: isize = redis::cmd("LPUSH")
            .arg(COMMIT_QUEUE_NAME)
            .arg(data)
            .query(&mut self.conn)
            .unwrap();
    }

    fn pop(&mut self) -> Option<Task> {
        let outcome: Option<Vec<u8>>;
        outcome = redis::cmd("LPOP")
            .arg(COMMIT_QUEUE_NAME)
            .query(&mut self.conn)
            .unwrap();

        match outcome {
            Some(data) => {
                // TODO: Support multiple task types
                let task: PostPushComplete = bincode::deserialize(&data).unwrap();
                Some(Task::PostPushComplete(task))
            }
            None => None,
        }
    }
}

impl RedisTaskQueue {
    pub fn new(conn: Connection) -> Self {
        RedisTaskQueue { conn }
    }
}

pub struct InMemoryTaskQueue {
    queue: Arc<Mutex<VecDeque<Task>>>,
}

impl TaskQueue for InMemoryTaskQueue {
    fn push(&mut self, task: Task) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(task);
    }

    fn pop(&mut self) -> Option<Task> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        InMemoryTaskQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }
}
