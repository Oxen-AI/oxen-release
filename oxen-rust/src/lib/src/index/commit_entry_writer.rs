
use crate::error::OxenError;
use crate::index::Committer;
use crate::model::{Commit, CommitEntry};

use filetime::FileTime;
use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};

use crate::model::LocalRepository;


pub struct CommitEntryWriter {
    db: DBWithThreadMode<MultiThreaded>,
}

impl CommitEntryWriter {
    pub fn db_opts() -> Options {
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Fatal);
        opts.create_if_missing(true);
        opts
    }

    pub fn new(repository: &LocalRepository, commit: &Commit) -> Result<CommitEntryWriter, OxenError> {
        let db_path = Committer::history_dir(&repository.path).join(commit.id.to_owned());
        let opts = CommitEntryWriter::db_opts();
        Ok(CommitEntryWriter {
            db: DBWithThreadMode::open(&opts, &db_path)?,
        })
    }

    pub fn set_file_timestamps(
        &self,
        entry: &CommitEntry,
        time: &FileTime
    ) -> Result<(), OxenError> {
        let key = entry.path.to_str().unwrap();
        let bytes = key.as_bytes();
        let entry = CommitEntry {
            id: entry.id.to_owned(),
            commit_id: entry.commit_id.to_owned(),
            path: entry.path.to_owned(),
            is_synced: entry.is_synced,
            hash: entry.hash.to_owned(),
            last_modified_seconds: time.unix_seconds(),
            last_modified_nanoseconds: time.nanoseconds()
        };

        let json_str = serde_json::to_string(&entry)?;
        let data = json_str.as_bytes();
        match self.db.put(bytes, data) {
            Ok(_) => Ok(()),
            Err(err) => {
                let err = format!("set_file_timestamps() Err: {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }
}