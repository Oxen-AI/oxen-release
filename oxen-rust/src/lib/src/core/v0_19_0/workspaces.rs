use crate::constants;
use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::db;
use crate::core::refs::RefWriter;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository, NewCommitBody, Workspace};
use crate::util;

use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, SingleThreaded};
use std::path::Path;

pub mod commit;
pub mod data_frames;
pub mod files;
pub mod status;

pub fn init_workspace_repo(
    repo: &LocalRepository,
    workspace_dir: impl AsRef<Path>,
) -> Result<LocalRepository, OxenError> {
    let workspace_dir = workspace_dir.as_ref();
    let oxen_hidden_dir = repo.path.join(constants::OXEN_HIDDEN_DIR);
    let workspace_hidden_dir = workspace_dir.join(constants::OXEN_HIDDEN_DIR);
    log::debug!("init_workspace_repo {workspace_hidden_dir:?}");
    util::fs::create_dir_all(&workspace_hidden_dir)?;

    let dirs_to_copy = vec![constants::HISTORY_DIR];

    for dir in dirs_to_copy {
        let oxen_dir = oxen_hidden_dir.join(dir);
        let target_dir = workspace_hidden_dir.join(dir);

        log::debug!("init_workspace_repo copying {dir} dir {oxen_dir:?} -> {target_dir:?}");
        if oxen_dir.is_dir() {
            util::fs::copy_dir_all(oxen_dir, target_dir)?;
        } else {
            util::fs::copy(oxen_dir, target_dir)?;
        }
    }

    LocalRepository::new(workspace_dir)
}
