use std::path::Path;

use crate::error::OxenError;
use crate::index::CommitDirReader;
use crate::index::{CommitSchemaRowIndex, SchemaReader, Stager};
use crate::media::tabular;
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::opts::RestoreOpts;
use crate::util::{self, resource};

pub fn restore(repo: &LocalRepository, opts: RestoreOpts) -> Result<(), OxenError> {
    if opts.staged {
        return restore_staged(repo, opts);
    }

    let path = opts.path;
    let commit = resource::get_commit_or_head(repo, opts.source_ref)?;
    let reader = CommitDirReader::new(repo, &commit)?;

    if let Some(entry) = reader.get_entry(&path)? {
        if util::fs::is_tabular(&entry.path) {
            // Custom logic to restore tabular
            restore_tabular(repo, &path, &commit, &entry)?;
        } else {
            // just copy data back over if !tabular
            restore_regular(repo, &path, &entry)?;
        }

        println!("Restored file {:?}", path);
        Ok(())
    } else {
        let error = format!("Could not restore file: {:?} does not exist", path);
        Err(OxenError::basic_str(error))
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
        let error = format!("Could not restore staged file: {:?} does not exist", path);
        Err(OxenError::basic_str(error))
    }
}

fn restore_regular(
    repo: &LocalRepository,
    path: &Path,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let version_path = util::fs::version_path(repo, entry);
    let working_path = repo.path.join(&path);
    std::fs::copy(version_path, working_path)?;
    Ok(())
}

fn restore_tabular(
    repo: &LocalRepository,
    path: &Path,
    commit: &Commit,
    entry: &CommitEntry,
) -> Result<(), OxenError> {
    let schema_reader = SchemaReader::new(repo, &commit.id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(&entry.path)? {
        let row_index_reader = CommitSchemaRowIndex::new(repo, &commit.id, &schema, &entry.path)?;
        let mut df = row_index_reader.entry_df()?;
        log::debug!("Got subset! {}", df);
        let working_path = repo.path.join(path);
        log::debug!("Write to {:?}", working_path);
        tabular::write_df(&mut df, working_path)?;
    } else {
        log::error!(
            "Could not restore tabular file, no schema found for file {:?}",
            entry.path
        );
    }
    Ok(())
}
