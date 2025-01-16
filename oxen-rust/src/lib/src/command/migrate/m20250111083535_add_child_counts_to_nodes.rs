use std::path::Path;

use super::Migrate;

use crate::config::RepositoryConfig;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::merkle_tree::node::{DirNode, EMerkleTreeNode, VNode};
use crate::model::{Commit, LocalRepository};

use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{repositories, util};

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
            let repo = LocalRepository::from_dir(path)?;
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

fn run_on_one_repo(repo: &LocalRepository) -> Result<(), OxenError> {
    let commits = repositories::commits::list_all(repo)?;
    for commit in commits {
        run_on_commit(repo, &commit)?;
    }

    Ok(())
}

fn run_on_commit(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    log::info!(
        "Running add_child_counts_to_nodes on commit: {} for repo: {:?}",
        commit.id,
        repo.path
    );
    let mut repo = repo.clone();

    let Some(mut root_node) = repositories::tree::get_root_with_children(&repo, commit)? else {
        return Err(OxenError::basic_str("Root node not found"));
    };

    // Iterate over the nodes, find the VNode and DirNode, and add the child counts

    // TODO: Need to add a clean function to write the nodes back to disk

    repo.set_min_version(MinOxenVersion::from_string("0.25.0")?);

    root_node.walk_tree_mut(|node| {
        match &mut node.node {
            EMerkleTreeNode::Directory(dir) => {
                // Fuck, how do we update all the children nodes too?
                let child_count = node.children.len() as u64;
                let opts = dir.get_opts();
                let mut new_dir = DirNode::new(&repo, opts).expect("Failed to create dir");
                new_dir.set_num_entries(child_count);
                *dir = new_dir;
            }
            EMerkleTreeNode::VNode(vnode) => {
                let opts = vnode.get_opts();
                let mut new_vnode = VNode::new(&repo, opts).expect("Failed to create vnode");
                new_vnode.set_num_entries(node.children.len() as u64);
                *vnode = new_vnode;
            }
            _ => {
                // pass, FileNode was not changed, so it is on the latest version
            }
        }
    });

    // Write the tree back to disk
    repositories::tree::write_tree(&repo, &root_node)?;

    // Set the oxen version to 0.25.0
    let mut config = RepositoryConfig::from_repo(&repo)?;
    config.min_version = Some("0.25.0".to_string());
    let path = util::fs::config_filepath(&repo.path);
    config.save(&path)?;

    Ok(())
}

// Add tests
#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        model::{merkle_tree::node::EMerkleTreeNode, MerkleHash},
        test,
    };
    use std::str::FromStr;

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
            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(old_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            repositories::tree::print_tree(&repo, &latest_commit)?;

            old_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(commit.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(dir.version(), MinOxenVersion::V0_19_0);
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(vnode.version(), MinOxenVersion::V0_19_0);
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            // Run the migration
            run_on_one_repo(&repo)?;

            let repo = LocalRepository::from_dir(&repo.path)?;
            let latest_commit = repositories::commits::latest_commit(&repo)?;
            let commit_node_version =
                repositories::tree::get_commit_node_version(&repo, &latest_commit)?;
            let node_version_str = commit_node_version.to_string();
            assert_eq!(node_version_str, "0.25.0");

            let commit_hash = MerkleHash::from_str(&latest_commit.id)?;
            let Some(new_root_node) =
                repositories::tree::get_node_by_id_with_children(&repo, &commit_hash)?
            else {
                return Err(OxenError::basic_str("Root node not found"));
            };

            new_root_node.walk_tree(|node| {
                println!("test_add_child_counts_to_nodes node: {}", node);
                match &node.node {
                    EMerkleTreeNode::Commit(commit) => {
                        assert_eq!(
                            commit.version(),
                            MinOxenVersion::from_string("0.25.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::Directory(dir) => {
                        assert_eq!(
                            dir.version(),
                            MinOxenVersion::from_string("0.25.0").unwrap()
                        );
                    }
                    EMerkleTreeNode::VNode(vnode) => {
                        assert_eq!(
                            vnode.version(),
                            MinOxenVersion::from_string("0.25.0").unwrap()
                        );
                    }
                    _ => {
                        // pass, FileNode was not changed, so it is on the latest version
                    }
                }
            });

            // Make sure that the repo version is updated
            let repo = LocalRepository::from_dir(&repo.path)?;
            let version_str = repo.min_version().to_string();
            assert_eq!(version_str, "0.25.0");

            Ok(())
        })
    }
}
