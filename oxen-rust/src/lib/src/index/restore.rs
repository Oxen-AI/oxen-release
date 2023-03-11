use filetime::FileTime;
use std::path::Path;

use crate::error::OxenError;
use crate::index::Stager;
use crate::index::{CommitDirEntryWriter, CommitDirReader};
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::opts::RestoreOpts;
use crate::util::{self, resource};

use super::CommitDirEntryReader;

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    if opts.staged {
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit = resource::get_commit_or_head(repo, opts.source_ref)?;
    let reader = CommitDirReader::new(repo, &commit)?;

    // Check if is directory, need to recursively restore
    if reader.has_dir(&path) {
        log::debug!("Restoring directory: {:?}", path);
        restore_dir(repo, &path, &commit, &reader)
    } else {
        // is file
        if let Some(entry) = reader.get_entry(&path)? {
            restore_file(repo, &path, &commit.id, &entry)
        } else {
            let error = format!("Could not restore file: {path:?} does not exist");
            Err(OxenError::basic_str(error))
        }
    }
}

fn restore_staged(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    let path = opts.path;
    log::debug!("restore_staged {:?}", path);

    let stager = Stager::new(repo)?;
    if stager.has_entry(&path) {
        stager.remove_staged_file(&path)
    } else if stager.has_staged_dir(&path) {
        stager.remove_staged_dir(&path)
    } else {
        let error = format!("Could not restore staged file: {path:?} does not exist");
        Err(OxenError::basic_str(error))
    }
}

fn restore_dir(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
    dir_reader: &CommitDirReader,
) -> Result<(), OxenError> {
    let dirs = dir_reader.list_committed_dirs()?;
    for dir in dirs {
        if dir.starts_with(path) {
            let reader = CommitDirEntryReader::new(repo, &commit.id, &dir)?;
            let entries = reader.list_entries()?;
            for entry in entries.iter() {
                restore_file(repo, &entry.path, &commit.id, entry)?;
            }
        }
    }

    Ok(())
}

pub fn restore_file(
    repo: &LocalRepository,
    path: &Path,
    commit_id: &str,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    // Update the local modified timestamps
    let dir = path.parent().unwrap();
    let committer = CommitDirEntryWriter::new(repo, commit_id, dir)?;
    restore_file_with_commit_writer(repo, path, entry, &committer)?;

    Ok(())
}

pub fn restore_file_with_commit_writer(
    repo: &LocalRepository,
    path: &Path,
    entry: &CommitEntry,
    committer: &CommitDirEntryWriter,
) -> Result<(), OxenError> {
    // copy data back over
    restore_regular(repo, path, entry)?;

    // Update the local modified timestamps
    let working_path = repo.path.join(path);
    let metadata = std::fs::metadata(working_path).unwrap();
    let mtime = FileTime::from_last_modification_time(&metadata);
    committer.set_file_timestamps(entry, &mtime).unwrap();

    Ok(())
}

fn restore_regular(
    repo: &LocalRepository,
    path: &Path,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    let working_path = repo.path.join(path);
    let parent = working_path.parent().unwrap();
    if !parent.exists() {
        std::fs::create_dir_all(parent)?;
    }

    log::debug!("Restore file: {:?}", entry.path);
    std::fs::copy(version_path, working_path)?;
    Ok(())
}
