use std::{thread, time};
use serde::{Deserialize, Serialize};
use liboxen::{core::cache::commit_cacher, model::{LocalRepository, Commit}};

#[derive(Serialize, Deserialize, Debug)]
pub struct PostPushComplete {
    pub commit: Commit,
    pub repo: LocalRepository
}

impl PostPushComplete {
    pub fn run(self) -> () {
        log::debug!("Running cachers for commit {:?} on repo {:?} from redis queue", self.commit.id, &self.repo.path);
        let duration = time::Duration::from_secs(1);
        println!("Here is the commit id: {}", self.commit.id);
        let force = false;
        match commit_cacher::run_all(&self.repo, &self.commit, force) {
            Ok(_) => {
                log::debug!("Cachers ran successfully for commit {:?} on repo {:?} from redis queue", self.commit.id, &self.repo.path);
            },
            Err(e) => {
                log::error!("Cachers failed to run for commit {:?} on repo {:?} from redis queue", self.commit.id, &self.repo.path);
                log::error!("Error: {:?}", e);
            }
        }
    }
}