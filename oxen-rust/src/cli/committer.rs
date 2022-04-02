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

    #[test]
    fn test_commit_staged() -> Result<(), OxenError> {
        

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(data_dirpath)?;

        Ok(())
    }
}
