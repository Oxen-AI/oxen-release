use async_recursion::async_recursion;
use bytesize::ByteSize;
use filetime::FileTime;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::{stream, StreamExt};
use indicatif::ProgressBar;
use rayon::prelude::*;
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::api;
use crate::constants::HISTORY_DIR;
use crate::error::OxenError;
use crate::index::{
    CommitDirEntryWriter, CommitDirReader, CommitReader, CommitWriter, RefReader, RefWriter,
};
use crate::model::{Commit, CommitEntry, LocalRepository, RemoteBranch, RemoteRepository};
use crate::util;

pub struct Indexer {
    pub repository: LocalRepository,
}

impl Indexer {
    pub fn new(repository: &LocalRepository) -> Result<Indexer, OxenError> {
        Ok(Indexer {
            repository: repository.clone(),
        })
    }

    pub async fn push(&self, rb: &RemoteBranch) -> Result<RemoteRepository, OxenError> {
        if !self.local_branch_exists(&rb.branch)? {
            return Err(OxenError::local_branch_not_found(&rb.branch));
        }

        println!("🐂 Oxen push {} {}", rb.remote, rb.branch);
        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or_else(OxenError::remote_not_set)?;

        log::debug!("Pushing to remote {:?}", remote);
        // Repo should be created before this step
        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        // Push unsynced commit db and history dbs
        let commit_reader = CommitReader::new(&self.repository)?;
        let head_commit = commit_reader.head_commit()?;

        // This method will check with server to find out what commits need to be pushed
        // will fill in commits that are not synced
        let mut unsynced_commits: VecDeque<Commit> = VecDeque::new();
        self.rpush_missing_commit_objects(&remote_repo, &head_commit, rb, &mut unsynced_commits)
            .await?;
        let last_commit = unsynced_commits.pop_front().unwrap();

        log::debug!(
            "Push entries for {} unsynced commits",
            unsynced_commits.len()
        );

        // recursively check commits against remote head
        // and sync ones that have not been synced
        self.rpush_entries(&remote_repo, &last_commit, &unsynced_commits)
            .await?;

        // update the branch after everything else is synced
        log::debug!(
            "Updating remote branch {:?} to commit {:?}",
            &rb.branch,
            &head_commit
        );
        api::remote::branches::update(&remote_repo, &rb.branch, &head_commit).await?;
        println!(
            "Updated remote branch {} to {}",
            &rb.branch, &head_commit.id
        );
        Ok(remote_repo)
    }

    fn local_branch_exists(&self, name: &str) -> Result<bool, OxenError> {
        let ref_reader = RefReader::new(&self.repository)?;
        Ok(ref_reader.has_branch(name))
    }

    fn read_num_local_entries(&self, commit: &Commit) -> Result<usize, OxenError> {
        let entry_reader = CommitDirReader::new(&self.repository, commit)?;
        entry_reader.num_entries()
    }

    #[async_recursion]
    async fn rpush_missing_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        local_commit: &Commit,
        rb: &RemoteBranch,
        unsynced_commits: &mut VecDeque<Commit>,
    ) -> Result<(), OxenError> {
        let num_entries = self.read_num_local_entries(local_commit)?;
        log::debug!(
            "rpush_missing_commit_objects START, checking local with {} entries {} -> '{}'",
            num_entries,
            local_commit.id,
            local_commit.message
        );

        // check if commit exists on remote
        // if not, push the commit and it's dbs
        match api::remote::commits::commit_is_synced(remote_repo, &local_commit.id, num_entries)
            .await
        {
            Ok(true) => {
                // We have remote commit, stop syncing
                log::debug!(
                    "rpush_missing_commit_objects STOP, we have remote parent {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );

                log::debug!(
                    "rpush_missing_commit_objects unsynced_commits.push_back root {:?}",
                    local_commit
                );
                // Add the last one because we are going to pop it off
                unsynced_commits.push_back(local_commit.to_owned());
            }
            Ok(false) => {
                log::debug!(
                    "rpush_missing_commit_objects CONTINUE Didn't find remote parent: {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );
                // We don't have remote commit
                // Recursively find local parent and remote parents
                for parent_id in local_commit.parent_ids.iter() {
                    // We should have a local parent if the local_commit has parent id
                    let local_parent = api::local::commits::get_by_id(&self.repository, parent_id)?
                        .ok_or_else(|| OxenError::local_parent_link_broken(&local_commit.id))?;

                    self.rpush_missing_commit_objects(
                        remote_repo,
                        &local_parent,
                        rb,
                        unsynced_commits,
                    )
                    .await?;

                    // Unroll and post commits
                    api::remote::commits::post_commit_to_server(
                        &self.repository,
                        remote_repo,
                        local_commit,
                    )
                    .await?;
                    log::debug!(
                        "rpush_missing_commit_objects unsynced_commits.push_back parent {:?}",
                        local_commit
                    );
                    unsynced_commits.push_back(local_commit.to_owned());
                }

                log::debug!(
                    "rpush_missing_commit_objects stop, no more local parents {} -> '{}'",
                    local_commit.id,
                    local_commit.message
                );

                if local_commit.parent_ids.is_empty() {
                    // Create the root commit
                    api::remote::commits::post_commit_to_server(
                        &self.repository,
                        remote_repo,
                        local_commit,
                    )
                    .await?;
                    log::debug!("unsynced_commits.push_back root {:?}", local_commit);
                    unsynced_commits.push_back(local_commit.to_owned());
                }
            }
            Err(err) => {
                let err = format!("Could not push missing commit err: {}", err);
                return Err(OxenError::basic_str(&err));
            }
        }

        Ok(())
    }

    async fn rpush_entries(
        &self,
        remote_repo: &RemoteRepository,
        head_commit: &Commit,
        unsynced_commits: &VecDeque<Commit>,
    ) -> Result<(), OxenError> {
        log::debug!("rpush_entries num unsynced {}", unsynced_commits.len());
        let mut last_commit = head_commit.clone();
        for commit in unsynced_commits.iter() {
            println!(
                "Pushing commit entries: {} -> '{}'",
                commit.id, commit.message
            );

            let entries = self.read_unsynced_entries(&last_commit, commit)?;
            if !entries.is_empty() {
                self.push_entries(remote_repo, &entries, commit).await?;
            }
            last_commit = commit.clone();
        }
        Ok(())
    }

    fn read_unsynced_entries(
        &self,
        last_commit: &Commit,
        this_commit: &Commit,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        println!("Computing delta {} -> {}", last_commit.id, this_commit.id);
        // In function scope to open and close this DB for a read, because we are going to write
        // to entries later
        let this_entry_reader = CommitDirReader::new(&self.repository, this_commit)?;
        let last_entry_reader = CommitDirReader::new(&self.repository, last_commit)?;

        let mut entries_to_sync: Vec<CommitEntry> = vec![];
        let this_entries = this_entry_reader.list_entries()?;
        let bar = ProgressBar::new(this_entries.len() as u64);
        for entry in this_entries {
            // If hashes are different, or it is a new entry, we'll push it
            if let Some(old_entry) = last_entry_reader.get_entry(&entry.path)? {
                if old_entry.hash != entry.hash {
                    entries_to_sync.push(entry);
                }
            } else {
                entries_to_sync.push(entry);
            }
            bar.inc(1);
        }
        println!("Got {} entries to sync", entries_to_sync.len());

        Ok(entries_to_sync)
    }

    async fn push_entries(
        &self,
        remote_repo: &RemoteRepository,
        entries: &[CommitEntry],
        commit: &Commit,
    ) -> Result<(), OxenError> {
        let mut total_size: u64 = 0;
        for entry in entries.iter() {
            // log::debug!("push [{}] adding entry to push {:?}", commit.id, entry);
            let version_path = util::fs::version_path(&self.repository, entry);
            match fs::metadata(&version_path) {
                Ok(metadata) => {
                    total_size += metadata.len();
                }
                Err(err) => {
                    log::error!("Err getting metadata on {:?}\n{:?}", version_path, err);
                }
            }
        }

        println!(
            "🐂 push {} files, compressing {}",
            entries.len(),
            ByteSize::b(total_size)
        );

        // We want each chunk to be ~= 5mb
        let avg_chunk_size = 500000;
        let num_chunks = ((total_size / avg_chunk_size) + 1) as usize;
        let bar = Arc::new(ProgressBar::new(total_size as u64));

        let mut chunk_size = entries.len() / num_chunks;
        if num_chunks > entries.len() {
            chunk_size = entries.len();
        }

        log::debug!("Creating {num_chunks} chunks from {total_size} bytes with size {chunk_size}");
        let chunks: Vec<&[CommitEntry]> = entries.chunks(chunk_size).collect();
        let results = stream::iter(chunks)
            .map(|chunk| {
                async move {
                    // 1) zip up entries into tarballs
                    let enc = GzEncoder::new(Vec::new(), Compression::fast());
                    let mut tar = tar::Builder::new(enc);
                    for entry in chunk.iter() {
                        let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
                        let version_path = util::fs::version_path(&self.repository, entry);
                        let name =
                            util::fs::path_relative_to_dir(&version_path, &hidden_dir).unwrap();

                        tar.append_path_with_name(version_path, name).unwrap();
                    }

                    // TODO: Clean this up... many places it could fail, but just want to get something working
                    tar.finish().unwrap();
                    let buffer: Vec<u8> = tar.into_inner().unwrap().finish().unwrap();
                    let size = buffer.len() as u64;

                    api::remote::commits::post_tarball_to_server(remote_repo, commit, buffer)
                        .await?;
                    futures::future::ok::<u64, OxenError>(size).await
                }
            })
            .buffer_unordered(num_cpus::get());

        results
            .for_each(|result| async {
                match result {
                    Ok(size) => bar.inc(size),
                    Err(e) => {
                        log::error!("Could not push entry: {}", e)
                    }
                }
            })
            .await;

        Ok(())
    }

    pub async fn pull(&self, rb: &RemoteBranch) -> Result<(), OxenError> {
        println!("🐂 Oxen pull {} {}", rb.remote, rb.branch);

        let remote = self
            .repository
            .get_remote(&rb.remote)
            .ok_or_else(OxenError::remote_not_set)?;

        let remote_repo = match api::remote::repositories::get_by_remote(&remote).await {
            Ok(Some(repo)) => repo,
            Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
            Err(err) => return Err(err),
        };

        if let Some(commit) = self.pull_all_commit_objects(&remote_repo, rb).await? {
            let limit: usize = 0; // zero means pull all
            self.pull_entries_for_commit(&remote_repo, &commit, limit)
                .await?;
        }
        Ok(())
    }

    pub async fn pull_all_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        rb: &RemoteBranch,
    ) -> Result<Option<Commit>, OxenError> {
        let remote_branch_err = format!("Remote branch not found: {}", rb.branch);
        let remote_branch = api::remote::branches::get_by_name(remote_repo, &rb.branch)
            .await?
            .ok_or_else(|| OxenError::basic_str(&remote_branch_err))?;
        match api::remote::commits::get_by_id(remote_repo, &remote_branch.commit_id).await {
            Ok(Some(commit)) => {
                log::debug!(
                    "Oxen pull got remote commit: {} -> '{}'",
                    commit.id,
                    commit.message
                );

                // Make sure this branch points to this commit
                self.set_branch_name_for_commit(&rb.branch, &commit)?;

                println!("🐂 fetching commit objects {}", commit.id);
                // Sync the commit objects
                self.rpull_missing_commit_objects(remote_repo, &commit)
                    .await?;
                return Ok(Some(commit));
            }
            Ok(None) => {
                eprintln!("oxen pull error: remote head does not exist");
            }
            Err(err) => {
                log::debug!("oxen pull could not get remote head: {}", err);
            }
        }

        Ok(None)
    }

    fn set_branch_name_for_commit(&self, name: &str, commit: &Commit) -> Result<(), OxenError> {
        let ref_writer = RefWriter::new(&self.repository)?;
        // Make sure head is pointing to that branch
        ref_writer.set_head(name);
        ref_writer.set_branch_commit_id(name, &commit.id)
    }

    /// Just pull the commit db and history dbs that are missing (not the entries)
    async fn rpull_missing_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        remote_head_commit: &Commit,
    ) -> Result<(), OxenError> {
        // See if we have the DB pulled
        let commit_db_dir = util::fs::oxen_hidden_dir(&self.repository.path)
            .join(HISTORY_DIR)
            .join(remote_head_commit.id.clone());
        if !commit_db_dir.exists() {
            // We don't have db locally, so pull it
            log::debug!(
                "commit db for {} not found, pull from remote",
                remote_head_commit.id
            );
            self.check_parent_and_pull_commit_objects(remote_repo, remote_head_commit)
                .await?;
        } else {
            // else we are synced
            log::debug!("commit db for {} already downloaded", remote_head_commit.id);
        }

        Ok(())
    }

    #[async_recursion]
    async fn check_parent_and_pull_commit_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        // If we have a parent on the remote
        if let Ok(parents) = api::remote::commits::get_remote_parent(remote_repo, &commit.id).await
        {
            // Recursively sync the parents
            for parent in parents.iter() {
                self.check_parent_and_pull_commit_objects(remote_repo, parent)
                    .await?;
            }
        }

        // Pulls dbs and commit object
        self.pull_commit_data_objects(remote_repo, commit).await?;

        Ok(())
    }

    async fn pull_commit_data_objects(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
    ) -> Result<(), OxenError> {
        log::debug!(
            "pull_commit_data_objects {} `{}`",
            commit.id,
            commit.message
        );

        // Download the specific commit_db that holds all the entries
        api::remote::commits::download_commit_db_by_id(&self.repository, remote_repo, &commit.id)
            .await?;

        // Get commit and write it to local DB
        let remote_commit = api::remote::commits::get_by_id(remote_repo, &commit.id)
            .await?
            .unwrap();
        let writer = CommitWriter::new(&self.repository)?;
        writer.add_commit_to_db(&remote_commit)
    }

    // For unit testing a half synced commit
    pub async fn pull_entries_for_commit_with_limit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        self.pull_commit_data_objects(remote_repo, commit).await?;
        self.pull_entries_for_commit(remote_repo, commit, limit)
            .await
    }

    fn read_pulled_commit_entries(
        &self,
        commit: &Commit,
        mut limit: usize,
    ) -> Result<Vec<CommitEntry>, OxenError> {
        let commit_reader = CommitDirReader::new(&self.repository, commit)?;
        let entries = commit_reader.list_entries()?;
        if limit == 0 {
            limit = entries.len();
        }
        Ok(entries[0..limit].to_vec())
    }

    fn get_missing_content_ids(&self, entries: &[CommitEntry]) -> (Vec<String>, u64) {
        let mut content_ids: Vec<String> = vec![];

        let mut size: u64 = 0;
        for entry in entries.iter() {
            let version_path = util::fs::version_path(&self.repository, entry);
            if !version_path.exists() {
                let version_path =
                    util::fs::path_relative_to_dir(&version_path, &self.repository.path).unwrap();
                content_ids.push(String::from(version_path.to_str().unwrap()));
                size += entry.num_bytes;
            }
        }

        (content_ids, size)
    }

    fn group_entries_to_parent_dirs(
        &self,
        files: &[CommitEntry],
    ) -> HashMap<PathBuf, Vec<CommitEntry>> {
        let mut results: HashMap<PathBuf, Vec<CommitEntry>> = HashMap::new();

        for entry in files.iter() {
            if let Some(parent) = entry.path.parent() {
                results
                    .entry(parent.to_path_buf())
                    .or_insert(vec![])
                    .push(entry.clone());
            }
        }

        results
    }

    async fn pull_entries_for_commit(
        &self,
        remote_repo: &RemoteRepository,
        commit: &Commit,
        limit: usize,
    ) -> Result<(), OxenError> {
        async fn join_parallel<T: Send + 'static>(
            futs: impl IntoIterator<Item = impl futures::Future<Output = T> + Send + 'static>,
        ) -> Vec<T> {
            let tasks: Vec<_> = futs.into_iter().map(tokio::spawn).collect();
            // unwrap the Result because it is introduced by tokio::spawn()
            // and isn't something our caller can handle
            futures::future::join_all(tasks)
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect()
        }

        let entries = self.read_pulled_commit_entries(commit, limit)?;
        log::debug!(
            "🐂 pull_entries_for_commit_id commit_id {} limit {} entries.len() {}",
            commit.id,
            limit,
            entries.len()
        );
        if !entries.is_empty() {
            let total = if limit > 0 { limit } else { entries.len() };
            println!("🐂 pulling commit {} with {} entries", commit.id, total);

            let (content_ids, size) = self.get_missing_content_ids(&entries);

            // We want each chunk to be ~= 5mb
            let avg_chunk_size = 500000;
            let num_chunks = ((size / avg_chunk_size) + 1) as usize;
            let bar = Arc::new(ProgressBar::new(size as u64));

            let mut chunk_size = entries.len() / num_chunks;
            if num_chunks > entries.len() {
                chunk_size = entries.len();
            }

            log::debug!(
                "pull_entries_for_commit got {} missing content IDs",
                content_ids.len()
            );

            // Chunk and run downloads in parallel
            let chunks: Vec<&[String]> = content_ids.chunks(chunk_size).collect();
            let results = stream::iter(chunks)
                .map(|chunk| {
                    let repo = self.repository.clone();

                    async move {
                        api::remote::entries::download_content_by_ids(&repo, remote_repo, chunk)
                            .await
                    }
                })
                // Number of CPUs will be number of par requests
                .buffer_unordered(num_cpus::get());

            // Collect results in progress bar, cannot `async move` progress bar above
            results
                .for_each(|result| async {
                    match result {
                        Ok(size) => bar.inc(size),
                        Err(err) => {
                            log::error!("Error downloading content... {:?}", err)
                        }
                    }
                })
                .await;
            bar.finish();

            println!("Unpacking...");
            let bar = Arc::new(ProgressBar::new(entries.len() as u64));
            let dir_entries = self.group_entries_to_parent_dirs(&entries);

            dir_entries.par_iter().for_each(|(dir, entries)| {
                let committer =
                    CommitDirEntryWriter::new(&self.repository, &commit.id, dir).unwrap();
                entries.par_iter().for_each(|entry| {
                    let filepath = self.repository.path.join(&entry.path);
                    if self.should_copy_entry(entry, &filepath) {
                        if let Some(parent) = filepath.parent() {
                            if !parent.exists() {
                                log::debug!("Create parent dir {:?}", parent);
                                std::fs::create_dir_all(parent).unwrap();
                            }
                        }

                        let version_path = util::fs::version_path(&self.repository, entry);
                        if std::fs::copy(&version_path, &filepath).is_err() {
                            eprintln!("Could not unpack file {:?} -> {:?}", version_path, filepath);
                        } else {
                            let metadata = fs::metadata(filepath).unwrap();
                            let mtime = FileTime::from_last_modification_time(&metadata);
                            committer.set_file_timestamps(entry, &mtime).unwrap();
                        }
                    }
                    bar.inc(1);
                });
            });

            bar.finish();
        }

        // Cleanup files that shouldn't be there
        self.cleanup_removed_entries(commit)?;

        Ok(())
    }

    fn cleanup_removed_entries(&self, commit: &Commit) -> Result<(), OxenError> {
        let commit_reader = CommitDirReader::new(&self.repository, commit)?;
        for file in util::fs::rlist_files_in_dir(&self.repository.path).iter() {
            let short_path = util::fs::path_relative_to_dir(file, &self.repository.path)?;
            if !commit_reader.has_file(&short_path) {
                std::fs::remove_file(file)?;
            }
        }
        Ok(())
    }

    fn should_copy_entry(&self, entry: &CommitEntry, path: &Path) -> bool {
        !path.exists() || self.path_hash_is_different(entry, path)
    }

    fn path_hash_is_different(&self, entry: &CommitEntry, path: &Path) -> bool {
        if let Ok(hash) = util::hasher::hash_file_contents(path) {
            log::debug!(
                "path_hash_is_different({:?})? {} == {}",
                entry.path,
                hash,
                entry.hash
            );
            return hash != entry.hash;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::constants;
    use crate::error::OxenError;
    use crate::index::Indexer;
    use crate::model::RemoteBranch;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_indexer_partial_pull_then_full() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|mut repo| async move {
            let og_num_files = util::fs::rcount_files_in_dir(&repo.path);

            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let remote_repo = command::create_remote(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let latest_commit = commits.first().unwrap();
                let page_size = 2;
                let limit = page_size;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, latest_commit, limit)
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                // try to pull the full thing again even though we have only partially pulled some
                let rb = RemoteBranch::default();
                indexer.pull(&rb).await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(og_num_files, num_files);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }

    #[tokio::test]
    async fn test_indexer_partial_pull_multiple_commits() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits_async(|mut repo| async move {
            // Set the proper remote
            let name = repo.dirname();
            let remote = test::repo_remote_url_from(&name);
            command::add_remote(&mut repo, constants::DEFAULT_REMOTE_NAME, &remote)?;

            let train_dir = repo.path.join("train");
            command::add(&repo, &train_dir)?;
            // Commit the file
            command::commit(&repo, "Adding training data")?;

            let test_dir = repo.path.join("test");
            command::add(&repo, &test_dir)?;
            // Commit the file
            command::commit(&repo, "Adding testing data")?;

            // Create remote
            let remote_repo = command::create_remote(
                &repo,
                constants::DEFAULT_NAMESPACE,
                &name,
                test::test_host(),
            )
            .await?;

            // Push it
            command::push(&repo).await?;

            test::run_empty_dir_test_async(|new_repo_dir| async move {
                let cloned_repo = command::clone(&remote_repo.remote.url, &new_repo_dir).await?;
                let indexer = Indexer::new(&cloned_repo)?;

                // Pull a part of the commit
                let commits = command::log(&repo)?;
                let last_commit = commits.first().unwrap();
                let limit = 7;
                indexer
                    .pull_entries_for_commit_with_limit(&remote_repo, last_commit, limit)
                    .await?;

                let num_files = util::fs::rcount_files_in_dir(&new_repo_dir);
                assert_eq!(num_files, limit);

                Ok(new_repo_dir)
            })
            .await
        })
        .await
    }
}
