use std::collections::HashMap;
use std::path::Path;

use jwalk::WalkDir;

use crate::constants::{HASH_FILE, VERSIONS_DIR, VERSION_FILE_NAME};
use crate::core::index::{CommitEntryReader, CommitReader};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util::fs::version_dir_from_hash;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, util};

pub trait Migrate {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError>;
    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError>;
}

pub struct UpdateVersionFilesMigration;

impl Migrate for UpdateVersionFilesMigration {
    fn up(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            update_version_files_for_all_repos_up(path)?;
        } else {
            let repo = LocalRepository::new(path)?;
            update_version_files_up(&repo)?;
        }
        Ok(())
    }

    fn down(&self, path: &Path, all: bool) -> Result<(), OxenError> {
        if all {
            update_version_files_for_all_repos_down(path)?;
        } else {
            println!("Running down migration");
            let repo = LocalRepository::new(path)?;
            update_version_files_down(&repo)?;
        }
        Ok(())
    }
}

pub fn update_version_files_for_all_repos_up(path: &Path) -> Result<(), OxenError> {
    let namespaces = api::local::repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match update_version_files_up(&repo) {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }

    Ok(())
}

pub fn update_version_files_up(repo: &LocalRepository) -> Result<(), OxenError> {
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let versions_dir = hidden_dir.join(VERSIONS_DIR);

    for entry in WalkDir::new(&versions_dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // Rename all files except for server-computed HASH
                if path.is_file() && path.file_name().unwrap() != HASH_FILE {
                    let new_path = util::fs::replace_file_name_keep_extension(
                        &path,
                        VERSION_FILE_NAME.to_owned(),
                    );
                    std::fs::rename(path, new_path)?;
                }
            }
            Err(err) => {
                log::error!("Error walking directory {:?}\nErr: {}", versions_dir, err);
            }
        }
    }

    Ok(())
}

pub fn update_version_files_down(repo: &LocalRepository) -> Result<(), OxenError> {
    // Hash map of entry hash (string) to path to write (commit id + extension)
    // (hash, extension) -> Vec<CommitId>
    let mut entry_hash_to_commit_ids: HashMap<(String, String), Vec<String>> = HashMap::new();
    // Get all commits for repo
    let commit_reader = CommitReader::new(repo)?;
    let all_commits = commit_reader.list_all()?;

    // Collect every commit in which a given hash + extension combination appears
    for commit in all_commits {
        let commit_entry_reader = CommitEntryReader::new(repo, &commit)?;
        let entries = commit_entry_reader.list_entries()?;
        for entry in entries {
            let entry_hash = entry.hash.clone().to_owned();
            let extension = entry.extension().to_owned();
            let commit_id = commit.id.to_owned();
            if entry_hash_to_commit_ids.contains_key(&(entry_hash.clone(), extension.clone())) {
                let commit_ids = entry_hash_to_commit_ids
                    .get_mut(&(entry_hash, extension))
                    .unwrap();
                commit_ids.push(commit_id);
            } else {
                let commit_ids = vec![commit_id];
                entry_hash_to_commit_ids.insert((entry_hash, extension), commit_ids);
            }
        }
    }

    // Iterate over these, copying the new-format data.extension file to commit_id.extension for all
    // commit ids, then delete new file
    for ((hash, extension), commit_ids) in entry_hash_to_commit_ids.iter() {
        let version_dir = version_dir_from_hash(&repo.path, hash.to_string());
        let new_filename = if extension.is_empty() {
            version_dir.join(VERSION_FILE_NAME)
        } else {
            version_dir.join(format!("{}.{}", VERSION_FILE_NAME, extension))
        };

        println!("Looking for new version file in {:?}", new_filename);
        if new_filename.exists() {
            for commit_id in commit_ids {
                let old_filename = version_dir.join(format!("{}.{}", commit_id, extension));
                println!("Copying {:?} to {:?}", new_filename, old_filename);
                std::fs::copy(new_filename.clone(), old_filename)?;
            }
            // Delete the new-format file
            std::fs::remove_file(new_filename)?;
        } else {
            Err(OxenError::basic_str(
                "Could not find version file, migration failed.",
            ))?;
        }
    }

    Ok(())
}

pub fn update_version_files_for_all_repos_down(path: &Path) -> Result<(), OxenError> {
    let namespaces = api::local::repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    println!("🐂 Migrating {} namespaces", namespaces.len());
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            match update_version_files_down(&repo) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(
                        "Could not down-migrate version files for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
        }
        bar.inc(1);
    }

    Ok(())
}
