use std::path::Path;

use jwalk::WalkDir;

use crate::constants::{VERSIONS_DIR, VERSION_FILE_NAME};
use crate::error::OxenError;
use crate::model::LocalRepository;
use crate::util::progress_bar::{oxen_progress_bar, ProgressBarType};
use crate::{api, util};

pub fn update_version_files_for_all_repos(path: &Path) -> Result<(), OxenError> {
    // Temporarily don't use path at all 
    let namespaces = api::local::repositories::list_namespaces(path)?;
    let bar = oxen_progress_bar(namespaces.len() as u64, ProgressBarType::Counter);
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            println!("Compute cache for repo {:?}", repo.path);
            match update_version_files(&repo) {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!(
                        "Could not migrate version files for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
            bar.inc(1);
        }
    }

    Ok(())
}

pub fn update_version_files(repo: &LocalRepository) -> Result<(), OxenError> {
    // Versions directory
    let hidden_dir = util::fs::oxen_hidden_dir(&repo.path);
    let versions_dir = hidden_dir.join(VERSIONS_DIR);

    for entry in WalkDir::new(&versions_dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if path.is_file() {
                    let new_path = util::fs::replace_file_name_keep_extension(
                        &path,
                        VERSION_FILE_NAME.to_owned(),
                    );
                    std::fs::rename(path, new_path)?;
                }
            }
            Err(err) => {
                log::error!("Error walking directory {:?}\nErr: {}", versions_dir, err);
            }
        }
    }

    Ok(())
}
