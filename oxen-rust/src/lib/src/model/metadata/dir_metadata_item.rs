use crate::api;
use crate::core::index::CommitReader;
use crate::model::metadata::to_duckdb_sql::ToDuckDBSql;
use crate::model::schema::{DataType, Field};
use crate::model::CommitEntry;
use crate::model::LocalRepository;
use crate::model::Schema;
use crate::util;

use duckdb::types::ToSql;
use std::path::Path;

pub struct DirMetadataItem {
    hash: String,
    directory: String,
    filename: String,
    path: String,
    num_bytes: u64,
    commit_id: String,
    timestamp: String,
    data_type: String,
    mime_type: String,
    extension: String,
}

impl DirMetadataItem {
    pub fn schema() -> Schema {
        let fields = vec![
            Field {
                name: "hash".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "directory".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "filename".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "path".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "num_bytes".to_string(),
                dtype: DataType::UInt64.to_string(),
            },
            Field {
                name: "commit_id".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "timestamp".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "data_type".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "mime_type".to_string(),
                dtype: DataType::String.to_string(),
            },
            Field {
                name: "extension".to_string(),
                dtype: DataType::String.to_string(),
            },
        ];
        Schema::new("metadata", fields)
    }

    pub fn from_entry(
        repo: &LocalRepository,
        entry: &CommitEntry,
        commit_reader: &CommitReader,
    ) -> Self {
        let path = util::fs::version_path(repo, entry);
        let mime_type = util::fs::file_mime_type(&path);
        let data_type = util::fs::datatype_from_mimetype(&path, &mime_type);

        // TODO: Handle unwraps more gracefully
        let size = api::local::metadata::get_file_size(&path).unwrap_or(0);
        let dir = entry
            .path
            .parent()
            .unwrap_or(Path::new(""))
            .to_string_lossy();
        let filename = entry.path.file_name().unwrap().to_str().unwrap();
        let entry_path = entry.path.to_string_lossy();
        let extension = util::fs::file_extension(&path);
        let data_type = format!("{:?}", data_type);
        let commit_id = entry.commit_id.clone();
        let commit = commit_reader.get_commit_by_id(&commit_id).unwrap().unwrap();
        let timestamp = commit
            .timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap();

        DirMetadataItem {
            hash: entry.hash.clone(),
            directory: dir.to_string(),
            filename: filename.to_string(),
            path: entry_path.to_string(),
            num_bytes: size,
            commit_id,
            timestamp,
            data_type,
            mime_type,
            extension,
        }
    }
}

impl ToDuckDBSql for DirMetadataItem {
    fn to_sql(&self) -> Vec<&dyn ToSql> {
        vec![
            &self.hash,
            &self.directory,
            &self.filename,
            &self.path,
            &self.num_bytes,
            &self.commit_id,
            &self.timestamp,
            &self.data_type,
            &self.mime_type,
            &self.extension,
        ]
    }
}
