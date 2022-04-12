
use crate::model::{Repository, RepositoryNew, ListRepositoriesResponse};
use crate::api;
use crate::error::OxenError;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn get_sync_dir() -> Result<PathBuf, OxenError> {
    let sync_dir_name = env::var("SYNC_DIR")?;
    let sync_dir = Path::new(&sync_dir_name);
    if !sync_dir.exists() {
        std::fs::create_dir_all(&sync_dir)?;
    }
    Ok(PathBuf::from(sync_dir))
}

pub fn list() -> Result<ListRepositoriesResponse, OxenError> {
    
    let name = String::from("test/example");
    let url = api::endpoint::url_from(&name);

    // TODO: list directories from sync dir as repos
    let mut repos: Vec<Repository> = vec![];
    let sync_dir = get_sync_dir()?;
    for entry in fs::read_dir(sync_dir)? {
        let local_path = entry?.path();
        if let Some(path_str) = local_path.to_str() {
            // TODO: get actual ID
            let id = format!("{}", uuid::Uuid::new_v4());
            repos.push(Repository {
                id: id,
                name: String::from(path_str),
                url: url.clone(),
            });
        }
    }

    Ok(ListRepositoriesResponse {
        repositories: repos
    })
}

pub fn create(repo: &RepositoryNew) -> Result<Repository, OxenError> {
    let id = format!("{}", uuid::Uuid::new_v4());
    let url = api::endpoint::url_from(&repo.name);

    let sync_dir = get_sync_dir()?;
    let repo_dir = sync_dir.join(Path::new(&repo.name));
    if !repo_dir.exists() {
        std::fs::create_dir_all(&repo_dir)?;
    }

    Ok(Repository {
        id: id,
        name: String::from(&repo.name),
        url: url,
    })
}

#[cfg(test)]
mod tests {

    use crate::api;
    use crate::error::OxenError;
    use crate::model::RepositoryNew;
    use std::env;
    use std::fs;
    use std::path::Path;

    fn setup_env(sync_dir: &str) {
        env::set_var("SYNC_DIR", &sync_dir);
        env::set_var("HOST", "0.0.0.0");
        env::set_var("PORT", "2000");
    }

    #[test]
    fn test_create_repository() -> Result<(), OxenError> {
        let sync_dir = format!("/tmp/oxen/test_sync_dir/{}", uuid::Uuid::new_v4());
        setup_env(&sync_dir);

        let name: &str = "testing";
        let repo = RepositoryNew {
            name: String::from(name)
        };
        let repository = api::local::repositories::create(&repo)?;
        assert_eq!(repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert_eq!(repo_path.exists(), true);

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }

    #[test]
    fn test_create_repository_path() -> Result<(), OxenError> {
        let sync_dir = format!("/tmp/oxen/test_sync_dir/{}", uuid::Uuid::new_v4());
        setup_env(&sync_dir);

        let name: &str = "gschoeni/CatsVsDogs";
        let repo = RepositoryNew {
            name: String::from(name)
        };
        let repository = api::local::repositories::create(&repo)?;
        assert_eq!(repository.name, name);

        let repo_path = Path::new(&sync_dir).join(Path::new(name));
        assert_eq!(repo_path.exists(), true);

        // cleanup
        fs::remove_dir_all(sync_dir)?;
        Ok(())
    }
}