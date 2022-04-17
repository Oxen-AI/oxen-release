use crate::cli::indexer::OXEN_HIDDEN_DIR;

use crate::error::OxenError;
use crate::util::FileUtil;

use rocksdb::{Options, DB, LogLevel};
use std::path::{Path, PathBuf};
use std::str;

pub const HEAD_FILE: &str = "HEAD";
pub const REFS_DIR: &str = "refs";
pub const DEFAULT_BRANCH: &str = "main";

pub struct Referencer {
    refs_db: DB,
    head_file: PathBuf,
}

impl Referencer {
    pub fn new(repo_dir: &Path) -> Result<Referencer, OxenError> {
        let refs_dir = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(REFS_DIR)));
        let head_file = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HEAD_FILE)));

        if !head_file.exists() {
            FileUtil::write_to_path(&head_file, DEFAULT_BRANCH);
        }

        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Warn);
        opts.create_if_missing(true);
        Ok(Referencer {
            refs_db: DB::open(&opts, &refs_dir)?,
            head_file,
        })
    }

    pub fn new_read_only(repo_dir: &Path) -> Result<Referencer, OxenError> {
        let refs_dir = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(REFS_DIR)));
        let head_file = repo_dir.join(Path::new(OXEN_HIDDEN_DIR).join(Path::new(HEAD_FILE)));

        if !head_file.exists() {
            FileUtil::write_to_path(&head_file, DEFAULT_BRANCH);
        }

        let error_if_log_file_exist = false;
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Warn);
        opts.create_if_missing(true);
        Ok(Referencer {
            refs_db: DB::open_for_read_only(&opts, &refs_dir, error_if_log_file_exist)?,
            head_file,
        })
    }

    pub fn set_head(&self, name: &str, commit_id: &str) -> Result<(), OxenError> {
        FileUtil::write_to_path(&self.head_file, name);
        self.refs_db.put(name, commit_id)?;
        Ok(())
    }

    pub fn get_commit_id(&self, name: &str) -> Result<String, OxenError> {
        let bytes = name.as_bytes();
        match self.refs_db.get(bytes) {
            Ok(Some(value)) => Ok(String::from(str::from_utf8(&*value)?)),
            Ok(None) => {
                let err = format!("ref not found: {}", name);
                Err(OxenError::basic_str(&err))
            }
            Err(err) => {
                let err = format!("{}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn head_commit_id(&self) -> Result<String, OxenError> {
        self.get_commit_id(&self.read_head()?)
    }

    pub fn read_head(&self) -> Result<String, OxenError> {
        FileUtil::read_from_path(&self.head_file)
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::test;

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_default_head() -> Result<(), OxenError> {
        let (referencer, repo_path) = test::create_referencer(BASE_DIR)?;

        assert_eq!(
            referencer.read_head()?,
            crate::cli::referencer::DEFAULT_BRANCH
        );

        // Cleanup
        std::fs::remove_dir_all(&repo_path)?;

        Ok(())
    }

    #[test]
    fn test_set_read_head() -> Result<(), OxenError> {
        let (referencer, repo_path) = test::create_referencer(BASE_DIR)?;

        let branch_name = "experiment/cat-dog";
        let commit_id = format!("{}", uuid::Uuid::new_v4());
        referencer.set_head(branch_name, &commit_id)?;
        assert_eq!(referencer.read_head()?, branch_name);
        assert_eq!(referencer.get_commit_id(branch_name)?, commit_id);

        // Cleanup
        std::fs::remove_dir_all(&repo_path)?;

        Ok(())
    }

    #[test]
    fn test_head_commit_id() -> Result<(), OxenError> {
        let (referencer, repo_path) = test::create_referencer(BASE_DIR)?;

        let branch_name = "experiment/cat-dog";
        let commit_id = format!("{}", uuid::Uuid::new_v4());
        referencer.set_head(branch_name, &commit_id)?;
        assert_eq!(referencer.head_commit_id()?, commit_id);

        // Cleanup
        std::fs::remove_dir_all(&repo_path)?;

        Ok(())
    }
}
