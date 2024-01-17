use std::path::Path;

use crate::{
    api,
    constants::{LAST_MIGRATION_FILE, OXEN_HIDDEN_DIR},
    error::OxenError,
    view::RepositoryView,
};

pub fn list_unmigrated(
    data_dir: &Path,
    migration_tstamp: String,
) -> Result<Vec<RepositoryView>, OxenError> {
    let mut result: Vec<RepositoryView> = vec![];

    let global_last_migration = data_dir.join(crate::constants::LAST_MIGRATION_FILE);

    if !global_last_migration.exists() {
        return Err(OxenError::basic_str(
            "No global migration file found on server.",
        ));
    }

    // Read this into a string
    let global_last_migration = std::fs::read_to_string(&global_last_migration)?;

    if global_last_migration >= migration_tstamp {
        log::debug!(
            "Global last migration file indicates all files successfully migrated up to {}",
            migration_tstamp
        );
        return Ok(result);
    }

    let namespaces = api::local::namespaces::list(data_dir);
    for namespace in namespaces {
        let namespace_path = data_dir.join(&namespace);
        for repo in api::local::repositories::list_repos_in_namespace(&namespace_path) {
            let repo_name = repo.path.file_name().unwrap().to_str().unwrap();

            let repo_last_migration = repo.path.join(OXEN_HIDDEN_DIR).join(LAST_MIGRATION_FILE);

            // If no repo-level migration file, the repo is migrated up to the global migration.
            // If we're here, that's out of date.
            if !repo_last_migration.exists() {
                log::debug!(
                    "No repo migration file found for {} looking in {}",
                    repo_name,
                    repo_last_migration.to_string_lossy()
                );
                result.push(RepositoryView {
                    namespace: namespace.clone(),
                    name: repo_name.to_string(),
                });
            } else if let Ok(repo_last_migration) = std::fs::read_to_string(&repo_last_migration) {
                if repo_last_migration <= migration_tstamp {
                    log::debug!("Repo migration file found for {} is out of date", repo_name);
                    result.push(RepositoryView {
                        namespace: namespace.clone(),
                        name: repo_name.to_string(),
                    });
                }
            }
        }
    }

    Ok(result)
}
