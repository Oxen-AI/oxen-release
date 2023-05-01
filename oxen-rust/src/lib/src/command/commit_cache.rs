//! # oxen commit-cache
//!
//! Compute the cache for a commits
//!

use std::path::Path;

use crate::{api, cache, error::OxenError, model::LocalRepository, opts::LogOpts};

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache_on_all_repos(path: &Path) -> Result<(), OxenError> {
    let namespaces = api::local::repositories::list_namespaces(path)?;
    for namespace in namespaces {
        let namespace_path = path.join(namespace);
        let repos = api::local::repositories::list_repos_in_namespace(&namespace_path);
        for repo in repos {
            println!("Compute cache for repo {:?}", repo.path);
            match compute_cache(&repo, None).await {
                Ok(_) => {
                    println!("Done.");
                }
                Err(err) => {
                    log::error!(
                        "Could not compute cache for repo {:?}\nErr: {}",
                        repo.path,
                        err
                    )
                }
            }
        }
    }

    Ok(())
}

/// Run the computation cache on all repositories within a directory
pub async fn compute_cache(
    repo: &LocalRepository,
    committish: Option<String>,
) -> Result<(), OxenError> {
    println!(
        "Compute cache for commit given [{committish:?}] on repo {:?}",
        repo.path
    );
    let commits = if let Some(committish) = committish {
        let opts = LogOpts {
            committish: Some(committish),
            remote: false,
        };
        api::local::commits::list_with_opts(repo, &opts).await?
    } else {
        api::local::commits::list(repo)?
    };
    for commit in commits {
        println!("Compute cache for commit {:?}", commit);
        cache::commit_cacher::run_all(repo, &commit)?;
    }
    Ok(())
}
