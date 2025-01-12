use std::path::Path;

use super::Migrate;

use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::LocalRepository;

use crate::repositories;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};

pub struct AddChildCountsToNodesMigration;
impl AddChildCountsToNodesMigration {}

impl Migrate for AddChildCountsToNodesMigration {
    fn name(&self) -> &'static str {
        "add_child_counts_to_nodes"
    }

    fn description(&self) -> &'static str {
        "Re-writes merkle tree with child counts for all directories and vnode nodes"
    }

    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            run_on_all_repos(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            run_on_one_repo(&repo)?;
        }
        Ok(())
    }

    fn down(&self, _path: &Path, _all: bool) -> Result<(), OxenError> {
        panic!("Not implemented");
    }

    fn is_needed(&self, repo: &LocalRepository) -> Result<bool, OxenError> {
        let latest_commit = repositories::commits::latest_commit(repo)?;
        let commit_node_version =
            repositories::tree::get_commit_node_version(repo, &latest_commit)?;
        Ok(commit_node_version == MinOxenVersion::V0_19_0)
    }
}

pub fn run_on_all_repos(path: &Path) -> Result<(), OxenError> {
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
            match run_on_one_repo(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
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

pub fn run_on_one_repo(_repo: &LocalRepository) -> Result<(), OxenError> {
    Ok(())
}

// Add tests
#[cfg(test)]
mod tests {
    use super::*;

    use crate::test;

    #[test]
    fn test_add_child_counts_to_nodes_migration() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            // Instantiate an older repository
            let repo = repositories::init::init_with_version(dir, MinOxenVersion::V0_19_0)?;

            // Populate the repo with some files
            test::populate_dir_with_training_data(&repo.path)?;
            // Make a variety of commits
            test::make_many_commits(&repo)?;

            // Test that the root commit
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_node_version =
                repositories::tree::get_commit_node_version(&repo, &latest_commit)?;

            // TODO: Since the writers always assume the latest version,
            // this doesn't work. Either the writers need to accept a version,
            // or we need to make a little v0.19.0 repository and check it in to test...
            // making the writers work with a version is the more robust solution, because then we can still
            // write to old versions of the repository.
            assert_eq!(commit_node_version, MinOxenVersion::V0_19_0);

            // Run the migration
            run_on_one_repo(&repo)?;

            let repo = LocalRepository::new(&repo.path)?;
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_node_version =
                repositories::tree::get_commit_node_version(&repo, &latest_commit)?;
            let node_version_str = commit_node_version.to_string();
            assert_eq!(node_version_str, "0.25.0");

            Ok(())
        })
    }
}
