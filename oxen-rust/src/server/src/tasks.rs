pub mod post_push_complete;

pub trait Runnable {
    fn run(&self);
}

#[derive(Debug)]
pub enum Task {
    PostPushComplete(post_push_complete::PostPushComplete),
}

impl Runnable for Task {
    fn run(&self) {
        match self {
            Task::PostPushComplete(task) => task.run(),
        }
    }
}
