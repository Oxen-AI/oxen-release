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
        let db_dir = format!("/tmp/oxen/db_{}", uuid::Uuid::new_v4());
        let db_path = Path::new(&db_dir);

        let data_dir = format!("/tmp/oxen/data_{}", uuid::Uuid::new_v4());
        let data_dirpath = PathBuf::from(&data_dir);
        std::fs::create_dir_all(&data_dirpath)?;

        let stager = Stager::new(db_path, &data_dirpath)?;

        // Make sure we have a valid file
        let hello_file = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&hello_file)?;
        file.write_all(b"Hello, world!")?;

        match stager.add_file(&hello_file) {
            Ok(path) => {
                if let Some(full_path) = hello_file.canonicalize()?.to_str() {
                    assert_eq!(path, full_path);
                } else {
                    panic!("test_add_file() Did not return full path")
                }
            }
            Err(err) => {
                panic!("test_add_file() Should have returned path... {}", err)
            }
        }

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(data_dirpath)?;

        Ok(())
    }
}
