use crate::error::OxenError;
use crate::index::{CommitDirEntryReader, CommitReader};
use crate::model::{Commit, CommitEntry, LocalRepository};
use crate::util;

use colored::Colorize;
use difference::{Changeset, Difference};
use std::path::Path;

use super::{CommitSchemaRowIndex, SchemaReader};

pub fn diff(
    repo: &LocalRepository,
    commit_id: Option<&str>,
    path: &str,
) -> Result<String, OxenError> {
    match _commit_or_head(repo, commit_id)? {
        Some(commit) => {
            let path = Path::new(path);
            _diff_commit(repo, &commit, path)
        }
        None => Err(OxenError::commit_id_does_not_exist(commit_id.unwrap())),
    }
}

fn _commit_or_head(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<Option<Commit>, OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    if let Some(commit_id) = commit_id {
        commit_reader.get_commit_by_id(commit_id)
    } else {
        Ok(Some(commit_reader.head_commit()?))
    }
}

// TODO: Change API to take two commits
fn _diff_commit(repo: &LocalRepository, commit: &Commit, path: &Path) -> Result<String, OxenError> {
    if let Some(parent) = path.parent() {
        let relative_parent = util::fs::path_relative_to_dir(parent, &repo.path)?;
        let commit_entry_reader = CommitDirEntryReader::new(repo, &commit.id, &relative_parent)?;
        let file_name = path.file_name().unwrap();
        if let Ok(Some(entry)) = commit_entry_reader.get_entry(file_name) {
            if util::fs::is_tabular(path) {
                let commit_reader = CommitReader::new(repo)?;
                let commits = commit_reader.history_from_head()?;

                let current_commit = commits.first().unwrap();

                return diff_tabular(repo, current_commit, &entry.path);
            } else if util::fs::is_utf8(path) {
                // TODO: Change API to take two commits
                return diff_utf8(repo, &entry);
            }
            Err(OxenError::basic_str(format!(
                "Diff not supported for file: {:?}",
                path
            )))
        } else {
            Err(OxenError::file_does_not_exist_in_commit(path, &commit.id))
        }
    } else {
        Err(OxenError::file_has_no_parent(path))
    }
}

pub fn diff_utf8(repo: &LocalRepository, entry: &CommitEntry) -> Result<String, OxenError> {
    let current_path = repo.path.join(&entry.path);
    let version_path = util::fs::version_path(repo, entry);

    let original = util::fs::read_from_path(&version_path)?;
    let modified = util::fs::read_from_path(&current_path)?;
    let Changeset { diffs, .. } = Changeset::new(&original, &modified, "\n");

    let mut outputs: Vec<String> = vec![];
    for diff in diffs {
        match diff {
            Difference::Same(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!(" {}\n", split).normal().to_string());
                }
            }
            Difference::Add(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("+{}\n", split).green().to_string());
                }
            }
            Difference::Rem(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("-{}\n", split).red().to_string());
                }
            }
        }
    }

    Ok(outputs.join(""))
}

pub fn diff_tabular(
    repo: &LocalRepository,
    current_commit: &Commit,
    path: &Path,
) -> Result<String, OxenError> {
    let schema_reader = SchemaReader::new(repo, &current_commit.id)?;
    if let Some(schema) = schema_reader.get_schema_for_file(path)? {
        let diff = CommitSchemaRowIndex::diff_current(repo, &schema, current_commit, path)?;
        let added_diff = format!("{}", diff.added_rows.unwrap());
        let removed_diff = format!("{}", diff.removed_rows.unwrap());
        Ok(format!(
            "Added Rows\n{added_diff}\n\nRemoved Rows\n{removed_diff}\n"
        ))
    } else {
        Err(OxenError::schema_does_not_exist_for_file(path))
    }
}
