//! Database for commit metadata entries.
//!
//! Metadata consists of the following fields:
//!
//! id: uint64
//! hash: str
//! directory: str
//! filename: str
//! path: str
//! num_bytes: uint64
//! commit_id: str
//! timestamp: str
//! data_type: str
//! mime_type: str
//! extension: str
//!

use crate::error::OxenError;
use crate::model::schema::{DataType, Field};
use crate::model::{Commit, LocalRepository, Schema};

use super::CommitEntryReader;

pub fn schema() -> Schema {
    let fields = vec![
        Field {
            name: "id".to_string(),
            dtype: DataType::UInt64.as_str().to_string(),
        },
        Field {
            name: "hash".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "directory".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "filename".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "path".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "num_bytes".to_string(),
            dtype: DataType::UInt64.as_str().to_string(),
        },
        Field {
            name: "commit_id".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "timestamp".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "data_type".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "mime_type".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
        Field {
            name: "extension".to_string(),
            dtype: DataType::String.as_str().to_string(),
        },
    ];
    Schema::new("metadata", fields)
}

pub fn insert_entries(repo: &LocalRepository, commit: &Commit) -> Result<(), OxenError> {
    // Read the commit entries
    let commit_entry_reader = CommitEntryReader::new(repo, commit)?;

    // We're going to compute types per directory, and save them into a dataframe
    let dirs = commit_entry_reader.list_dirs()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::test;

    use super::*;

    #[test]
    fn test_index_db() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| Ok(()))
    }
}
