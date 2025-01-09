use super::Migrate;

use crate::core::v0_10_0::index::CommitEntryWriter;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::{Commit, LocalRepository};
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{constants, repositories};
use std::path::Path;

pub struct CreateMerkleTreesMigration;
impl Migrate for CreateMerkleTreesMigration {
    fn name(&self) -> &'static str {
        "create_merkle_trees"
    }

    fn description(&self) -> &'static str {
        "Reformats the underlying data model into merkle trees for storage and lookup efficiency"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            create_merkle_trees_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            create_merkle_trees_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            create_merkle_trees_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            create_merkle_trees_down(&repo)?;
        }
        Ok(())
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let objects_dir = repo
            .path
            .join(constants::OXEN_HIDDEN_DIR)
            .join(constants::OBJECTS_DIR);
        if !objects_dir.exists() && repo.min_version() <= MinOxenVersion::V0_10_0 {
            return Ok(true);
        }
        // This may need a more elaborate check for migrations that are aborted with a single repo...
        // but it's too computationally expensive to parse through all the trees.
        Ok(false)
    }
}

pub fn create_merkle_trees_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    println!("🐂 Collecting namespaces to migrate...");
    let namespaces = repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        // Show the canonical namespace path
        log::debug!(
            "This is the namespace path we're walking: {:?}",
            namespace_path.canonicalize()?
        );
        let repos = repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match create_merkle_trees_up(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate merkle trees for repo {:?}\nErr: {}",
                        repo.path.canonicalize(),
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }
    Ok(())
}

pub fn create_merkle_trees_for_all_repos_down(_path: &Path) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn create_merkle_trees_up(_repo: &LocalRepository) -> Result<(), OxenError> {
    panic!("migrating to v0.10.0 is no longer supported");
}

pub fn create_merkle_trees_down(_repo: &LocalRepository) -> Result<(), OxenError> {
    println!("There are no operations to be run");
    Ok(())
}

pub fn construct_commit_merkle_tree_from_legacy(
    repo: &LocalRepository,
    commit: &Commit,
) -> Result<(), OxenError> {
    let commit_writer = CommitEntryWriter::new(repo, commit)?;
    commit_writer.construct_merkle_tree_from_legacy_commit(&repo.path)?;
    Ok(())
}
