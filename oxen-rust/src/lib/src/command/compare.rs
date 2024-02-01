use crate::api;
use crate::core::index::MergeConflictReader;
use crate::error::OxenError;
use crate::model::entry::commit_entry::{CommitPath, CompareEntry};
use crate::model::LocalRepository;
use crate::view::compare::CompareResult;
use std::path::PathBuf;

pub fn compare(
    repo: &LocalRepository,
    cpath_1: CommitPath,
    cpath_2: CommitPath,
    keys: Vec<String>,
    targets: Vec<String>,
    output: Option<PathBuf>,
) -> Result<(), OxenError> {
    // TODONOW - anything we can clean up with this mut initialization?
    let mut compare_entry_1 = CompareEntry {
        commit_entry: None,
        path: cpath_1.path.clone(),
    };

    let mut compare_entry_2 = CompareEntry {
        commit_entry: None,
        path: cpath_2.path.clone(),
    };

    if let Some(commit_1) = cpath_1.commit {
        let entry_1 = api::local::entries::get_commit_entry(repo, &commit_1, &cpath_1.path)?
            .ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath_1.path.display(), commit_1.id).into(),
                )
            })?;

        compare_entry_1.commit_entry = Some(entry_1);
    };

    if let Some(mut commit_2) = cpath_2.commit {
        // if there are merge conflicts, compare against the conflict commit instead
        let merger = MergeConflictReader::new(repo)?;

        if merger.has_conflicts()? {
            commit_2 = merger.get_conflict_commit()?.unwrap();
        }

        let entry_2 = api::local::entries::get_commit_entry(repo, &commit_2, &cpath_2.path)?
            .ok_or_else(|| {
                OxenError::ResourceNotFound(
                    format!("{}@{}", cpath_2.path.display(), commit_2.id).into(),
                )
            })?;

        compare_entry_2.commit_entry = Some(entry_2);
    };

    let compare_result = api::local::compare::compare_files(
        repo,
        None,
        compare_entry_1,
        compare_entry_2,
        keys,
        targets,
        output,
    )?;

    let text = match compare_result {
        CompareResult::Tabular((_, text)) => text,
        CompareResult::Text(text) => text,
    };
    println!("{}", text);
    Ok(())
}
