use crate::error::OxenError;
use crate::util::FileUtil;

use rocksdb::{IteratorMode, DB};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::str;

pub struct Committer {
    db: DB,
    repo_path: PathBuf,
}

impl Committer {
    pub fn new(dbpath: &Path, repo_path: &Path) -> Result<Committer, OxenError> {
        Ok(Committer {
            db: DB::open_default(dbpath)?,
            repo_path: repo_path.to_path_buf(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::stager::Stager;
    use crate::error::OxenError;
    use std::fs::File;
    use std::io::prelude::*;
    use std::path::{Path, PathBuf};

    const STAGED_REPO_DIR: &str = "data/test/repos/.oxen_staged";

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        let stager = Stager::new(&db_path, &repo_dir)?;

        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello World")?;

        // cleanup
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }
}
