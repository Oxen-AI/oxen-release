use std::sync::Mutex;
use std::{collections::VecDeque, sync::Arc};

use crate::tasks::post_push_complete::PostPushComplete;
use crate::tasks::Task;
use liboxen::constants::COMMIT_QUEUE_NAME;

#[derive(Clone)]
pub enum TaskQueue {
    InMemory(InMemoryTaskQueue),
    Redis(RedisTaskQueue),
}

impl TaskQueue {
    pub fn push(&mut self, task: Task) {
        match self {
            TaskQueue::InMemory(queue) => queue.push(task),
            TaskQueue::Redis(queue) => queue.push(task),
        }
    }

    pub fn pop(&mut self) -> Option<Task> {
        match self {
            TaskQueue::InMemory(queue) => queue.pop(),
            TaskQueue::Redis(queue) => queue.pop(),
        }
    }
}

#[derive(Clone)]
pub struct RedisTaskQueue {
    pub pool: r2d2::Pool<redis::Client>,
}

impl RedisTaskQueue {
    pub fn new(pool: r2d2::Pool<redis::Client>) -> Self {
        RedisTaskQueue { pool }
    }

    fn push(&mut self, task: Task) {
        let mut conn = self.pool.get().unwrap();

        let data: Vec<u8> = match task {
            Task::PostPushComplete(task) => bincode::serialize(&task).unwrap(),
        };

        let _: isize = redis::cmd("LPUSH")
            .arg(COMMIT_QUEUE_NAME)
            .arg(data)
            .query(&mut conn)
            .unwrap();
    }

    fn pop(&mut self) -> Option<Task> {
        let mut conn = self.pool.get().unwrap();
        let outcome: Option<Vec<u8>> = redis::cmd("LPOP")
            .arg(COMMIT_QUEUE_NAME)
            .query(&mut conn)
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

#[derive(Clone)]
pub struct InMemoryTaskQueue {
    queue: Arc<Mutex<VecDeque<Task>>>,
}

impl InMemoryTaskQueue {
    pub fn new() -> Self {
        InMemoryTaskQueue {
            queue: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn push(&mut self, task: Task) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(task);
    }

    fn pop(&mut self) -> Option<Task> {
        let mut queue = self.queue.lock().unwrap();
        queue.pop_front()
    }
}
impl Default for InMemoryTaskQueue {
    fn default() -> Self {
        Self::new()
    }
}
