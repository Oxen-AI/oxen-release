use filetime::FileTime;
use rocksdb::{DBWithThreadMode, MultiThreaded};
use std::path::Path;

use crate::api::local::resource;
use crate::core::db;
use crate::core::index::Stager;
use crate::core::index::{CommitDirEntryWriter, CommitEntryReader};
use crate::error::OxenError;
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::opts::RestoreOpts;
use crate::util;

use super::{CommitDirEntryReader, CommitEntryWriter, ObjectDBReader};

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    if opts.staged {
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit = resource::get_commit_or_head(repo, opts.source_ref)?;
    let reader = CommitEntryReader::new(repo, &commit)?;
    let opts = db::opts::default();
    let files_db_dir = CommitEntryWriter::files_db_dir(repo);
    let files_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&db::opts::default(), dunce::simplified(&files_db_dir))?;

    // Check if is directory, need to recursively restore
    if reader.has_dir(&path) {
        log::debug!("Restoring directory: {:?}", path);
        restore_dir(repo, &path, &commit, &reader, &files_db)
    } else {
        // is file
        if let Some(entry) = reader.get_entry(&path)? {
            restore_file(repo, &path, &commit.id, &entry, &files_db)
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
    dir_reader: &CommitEntryReader,
    files_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    let dirs = dir_reader.list_dirs()?;
    let object_reader = ObjectDBReader::new(repo)?;
    for dir in dirs {
        if dir.starts_with(path) {
            let reader = CommitDirEntryReader::new(repo, &commit.id, &dir, object_reader.clone())?;
            let entries = reader.list_entries()?;
            let msg = format!("Restoring Directory: {:?}", dir);
            let bar = util::progress_bar::oxen_progress_bar_with_msg(entries.len() as u64, &msg);

            // iterate over entries in parallel
            entries.iter().for_each(|entry| {
                match restore_file(repo, &entry.path, &commit.id, entry, &files_db) {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("Error restoring file {:?}: {:?}", entry.path, e);
                    }
                }
                bar.inc(1);
            });
            bar.finish_and_clear();
        }
    }

    Ok(())
}

pub fn restore_file(
    repo: &LocalRepository,
    path: &Path,
    commit_id: &str,
    entry: &CommitEntry,
    files_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    // Update the local modified timestamps
    let dir = path.parent().unwrap();
    let committer = CommitDirEntryWriter::new(repo, commit_id, dir)?;
    restore_file_with_commit_writer(repo, path, entry, &committer, files_db)?;

    Ok(())
}

pub fn restore_file_with_commit_writer(
    repo: &LocalRepository,
    path: &Path,
    entry: &CommitEntry,
    committer: &CommitDirEntryWriter,
    files_db: &DBWithThreadMode<MultiThreaded>,
) -> Result<(), OxenError> {
    // copy data back over
    restore_regular(repo, path, entry)?;

    // Update the local modified timestamps
    let working_path = repo.path.join(path);
    let metadata = util::fs::metadata(working_path).unwrap();
    let mtime = FileTime::from_last_modification_time(&metadata);
    committer
        .set_file_timestamps(entry, &mtime, files_db)
        .unwrap();

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
        util::fs::create_dir_all(parent)?;
    }

    util::fs::copy(version_path, working_path.clone())?;
    Ok(())
}
