use crate::constants::{REFS_DIR, HEAD_FILE};
use crate::error::OxenError;
use crate::model::{Branch, LocalRepository};
use crate::util;

use rocksdb::{IteratorMode, LogLevel, Options, DB};
use std::path::PathBuf;
use std::str;

pub struct RefReader {
    refs_db: DB,
    head_file: PathBuf,
}

impl RefReader {
    fn db_opts() -> Options {
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Fatal);
        opts.create_if_missing(true);
        opts
    }
    
    pub fn new(repository: &LocalRepository) -> Result<RefReader, OxenError> {
        let refs_dir = util::fs::oxen_hidden_dir(&repository.path).join(REFS_DIR);
        let head_filename = util::fs::oxen_hidden_dir(&repository.path).join(HEAD_FILE);

        let error_if_log_file_exist = false;
        let opts = RefReader::db_opts();
        Ok(RefReader {
            refs_db: DB::open_for_read_only(&opts, &refs_dir, error_if_log_file_exist)?,
            head_file: head_filename,
        })
    }

    pub fn has_branch(&self, name: &str) -> bool {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(_)) => true,
            Ok(None) => false,
            Err(_) => false,
        }
    }

    pub fn get_current_branch(&self) -> Result<Option<Branch>, OxenError> {
        let ref_name = self.read_head_ref()?;
        if let Some(id) = self.get_commit_id_for_branch(&ref_name)? {
            Ok(Some(Branch {
                name: ref_name,
                commit_id: id,
                is_head: true,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_commit_id_for_branch(&self, name: &str) -> Result<Option<String>, OxenError> {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(value)) => Ok(Some(String::from(str::from_utf8(&*value)?))),
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!("{}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn head_commit_id(&self) -> Result<String, OxenError> {
        let head_ref = self.read_head_ref()?;
        if let Some(commit_id) = self.get_commit_id_for_branch(&head_ref)? {
            Ok(commit_id)
        } else {
            Ok(head_ref)
        }
    }

    pub fn read_head_ref(&self) -> Result<String, OxenError> {
        // Should probably lock before reading...
        // but not a lot of parallel action going on here
        util::fs::read_from_path(&self.head_file)
    }

    pub fn list_branches(&self) -> Result<Vec<Branch>, OxenError> {
        let mut branch_names: Vec<Branch> = vec![];
        let head_ref = self.read_head_ref()?;
        let iter = self.refs_db.iterator(IteratorMode::Start);
        for (key, value) in iter {
            match (str::from_utf8(&*key), str::from_utf8(&*value)) {
                (Ok(key_str), Ok(value)) => {
                    let ref_name = String::from(key_str);
                    let id = String::from(value);
                    branch_names.push(Branch {
                        name: ref_name.clone(),
                        commit_id: id.clone(),
                        is_head: (ref_name == head_ref),
                    });
                }
                _ => {
                    eprintln!("Could not read utf8 val...")
                }
            }
        }
        Ok(branch_names)
    }

    pub fn get_branch_by_name(&self, name: &str) -> Result<Option<Branch>, OxenError> {
        let head_commit_id = self.head_commit_id()?;
        match self.get_commit_id_for_branch(name) {
            Ok(Some(commit_id)) => Ok(Some(Branch {
                name: name.to_string(),
                commit_id: commit_id.to_string(),
                is_head: commit_id == head_commit_id,
            })),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        }
    }
}