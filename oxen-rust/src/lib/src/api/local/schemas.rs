use std::path::Path;

use crate::api;

use crate::core::index::SchemaReader;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema};

pub fn list(repo: &LocalRepository, commit_id: Option<&str>) -> Result<Vec<Schema>, OxenError> {
    if let Some(commit_id) = commit_id {
        if let Some(commit) = api::local::commits::commit_from_branch_or_commit_id(repo, commit_id)?
        {
            let schema_reader = SchemaReader::new(repo, &commit.id)?;
            schema_reader.list_schemas()
        } else {
            Err(OxenError::commit_id_does_not_exist(commit_id))
        }
    } else {
        let head_commit = api::local::commits::head_commit(repo)?;
        let schema_reader = SchemaReader::new(repo, &head_commit.id)?;
        schema_reader.list_schemas()
    }
}

pub fn get(
    repo: &LocalRepository,
    commit_id: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> Result<Option<Schema>, OxenError> {
    let commit_id = commit_id.as_ref();
    let path = path.as_ref();
    if let Some(commit) = api::local::commits::commit_from_branch_or_commit_id(repo, commit_id)? {
        let schema_reader = SchemaReader::new(repo, &commit.id)?;
        schema_reader.get_schema_for_file(path)
    } else {
        Err(OxenError::commit_id_does_not_exist(commit_id))
    }
}
