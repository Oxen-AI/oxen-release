use std::collections::HashMap;
use std::path::PathBuf;

use crate::api;

use crate::core::index::SchemaReader;
use crate::error::OxenError;
use crate::model::{LocalRepository, Schema};

pub fn list(
    repo: &LocalRepository,
    commit_id: Option<&str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
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

pub fn list_from_ref(
    repo: &LocalRepository,
    revision: impl AsRef<str>,
    schema_ref: impl AsRef<str>,
) -> Result<HashMap<PathBuf, Schema>, OxenError> {
    let revision = revision.as_ref();
    let schema_ref = schema_ref.as_ref();
    if let Some(commit) = api::local::revisions::get(repo, revision)? {
        let schema_reader = SchemaReader::new(repo, &commit.id)?;
        schema_reader.list_schemas_for_ref(schema_ref)
    } else {
        Err(OxenError::revision_not_found(revision.into()))
    }
}
