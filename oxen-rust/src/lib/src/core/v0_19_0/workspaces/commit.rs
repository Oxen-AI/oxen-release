use crate::constants::STAGED_DIR;
use crate::core;
use crate::core::db;
use crate::core::refs::RefWriter;
use crate::error::OxenError;
use crate::model::{Commit, NewCommitBody, Workspace};
use crate::util;

use indicatif::ProgressBar;
use rocksdb::{DBWithThreadMode, SingleThreaded};

pub fn commit(
    workspace: &Workspace,
    new_commit: &NewCommitBody,
    branch_name: impl AsRef<str>,
) -> Result<Commit, OxenError> {
    let branch_name = branch_name.as_ref();
    let staged_db_path = util::fs::oxen_hidden_dir(&workspace.workspace_repo.path).join(STAGED_DIR);
    log::debug!(
        "0.19.0::workspaces::commit staged db path: {:?}",
        staged_db_path
    );
    let opts = db::key_val::opts::default();
    let staged_db: DBWithThreadMode<SingleThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&staged_db_path))?;

    let commit_progress_bar = ProgressBar::new_spinner();

    // Read all the staged entries
    let (dir_entries, _) = core::v0_19_0::status::read_staged_entries(
        &workspace.workspace_repo,
        &staged_db,
        &commit_progress_bar,
    )?;

    let commit = core::v0_19_0::index::commit_writer::commit_dir_entries(
        &workspace.base_repo,
        dir_entries,
        new_commit,
        &staged_db_path,
        &commit_progress_bar,
    )?;

    // Update the branch
    let ref_writer = RefWriter::new(&workspace.base_repo)?;
    let commit_id = commit.id.to_owned();
    ref_writer.set_branch_commit_id(branch_name, &commit_id)?;

    Ok(commit)
}
