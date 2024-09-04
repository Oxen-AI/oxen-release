use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::opts::RmOpts;
use crate::repositories;
use crate::util;
use crate::constants;
use crate::core::db;


use crate::model::Commit;
use crate::model::StagedEntryStatus;
use crate::constants::VERSIONS_DIR;
use crate::constants::STAGED_DIR;
use crate::model::EntryDataType;


use rmp_serde::Serializer;
use serde::Serialize;


use glob::glob;
use std::collections::HashSet;
use std::path::{Path, PathBuf};


use rocksdb::{DBWithThreadMode, MultiThreaded};


pub async fn rm(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {
    // not sure if this is important?
    if repo.is_shallow_clone() {
            return Err(OxenError::repo_is_shallow());
    }
    /*
    if opts.remote {
        return remove_remote(repo, opts).await;
    }    
    */


    if opts.staged {
        return remove_staged_files(repo, opts);
    }


    // If removing committed files, call add
    // Todo: Remove the files lol
    repositories::add(repo, &opts.path)
}






// Call remove_dir_or_file on every path matching glob
fn remove_staged_files(repo: &LocalRepository, opts: &RmOpts) -> Result<(), OxenError> {


    let path: &Path = opts.path.as_ref();
    let mut entries: HashSet<PathBuf> = HashSet::new();
    if let Some(path_str) = path.to_str() {
        if util::fs::is_glob_path(path_str) {
            // Match against any untracked entries in the current dir
            for entry in glob(path_str)? {
                entries.insert(entry?);
            }
        } else {
            // Non-glob path
            entries.insert(path.to_owned());
        }
    }


    for entry in entries {
        /*
        if entry.is_dir() {
            remove_staged_dir(entry.as_ref(), repo);
        }
        */
        remove_staged_file(entry.as_ref(), repo)?;
    }


    Ok(())
}


/*
fn remove_staged_dir(relative_path: &Path, repo: &LocalRepository, head_commit: &Commit)
    -> Result<EntryMetaData, OxenError> {
   
}
*/


fn remove_staged_file(
    relative_path: &Path,
    repo: &LocalRepository
) -> Result<(), OxenError> {


    let repo_path = &repo.path;
    let opts = db::key_val::opts::default();
    let db_path = util::fs::oxen_hidden_dir(&repo.path).join(STAGED_DIR);
    let staged_db: DBWithThreadMode<MultiThreaded> =
        DBWithThreadMode::open(&opts, dunce::simplified(&db_path))?;
   
    staged_db.delete(relative_path.to_str().unwrap())?;
    Ok(())
}





