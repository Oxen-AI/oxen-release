//! # oxen push
//!
//! Push data from your local machine to a remote.
//!

use crate::api;
use crate::error::OxenError;
use crate::index::EntryIndexer;
use crate::model::{Branch, LocalRepository, RemoteBranch, RemoteRepository};

/// # Get a log of all the commits
///
/// ```
/// # use liboxen::api;
/// # use liboxen::test;
/// use liboxen::command;
/// use liboxen::util;
/// # use liboxen::error::OxenError;
/// # use std::path::Path;
/// # #[tokio::main]
/// # async fn main() -> Result<(), OxenError> {
/// # test::init_test_env();
/// // Initialize the repository
/// let base_dir = Path::new("/tmp/repo_dir_push");
/// let mut repo = command::init(base_dir)?;
///
/// // Write file to disk
/// let hello_file = base_dir.join("hello.txt");
/// util::fs::write_to_path(&hello_file, "Hello World");
///
/// // Stage the file
/// command::add(&repo, &hello_file)?;
///
/// // Commit staged
/// command::commit(&repo, "My commit message")?;
///
/// // Set the remote server
/// command::add_remote(&mut repo, "origin", "http://localhost:3000/repositories/hello");
///
/// let remote_repo = command::create_remote(&repo, "repositories", "hello", "localhost:3000").await?;
///
/// // Push the file
/// command::push(&repo).await;
///
/// # std::fs::remove_dir_all(base_dir)?;
/// # api::remote::repositories::delete(&remote_repo).await?;
/// # Ok(())
/// # }
/// ```
pub async fn push(repo: &LocalRepository) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch::default();
    indexer.push(&rb).await
}

/// Push to a specific remote branch on the default remote repository
pub async fn push_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(repo)?;
    let rb = RemoteBranch {
        remote: String::from(remote),
        branch: String::from(branch),
    };
    indexer.push(&rb).await
}

/// Push to a specific remote repository
pub async fn push_remote_repo_branch(
    local_repo: LocalRepository,
    remote_repo: RemoteRepository,
    branch: Branch,
) -> Result<RemoteRepository, OxenError> {
    let indexer = EntryIndexer::new(&local_repo)?;
    indexer.push_remote_repo(remote_repo, branch).await
}

/// Push to a specific remote repository, given a branch name
pub async fn push_remote_repo_branch_name(
    local_repo: LocalRepository,
    remote_repo: RemoteRepository,
    branch_name: &str,
) -> Result<RemoteRepository, OxenError> {
    let branch = api::local::branches::get_by_name(&local_repo, branch_name)?
        .ok_or(OxenError::local_branch_not_found(branch_name))?;
    push_remote_repo_branch(local_repo, remote_repo, branch).await
}
