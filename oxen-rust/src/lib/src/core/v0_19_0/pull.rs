use crate::api;
use crate::error::OxenError;
use crate::model::{LocalRepository, RemoteBranch, RemoteRepository};
use crate::view::repository::RepositoryDataTypesView;

pub async fn pull(repo: &LocalRepository) -> Result<(), OxenError> {
    let rb = RemoteBranch::default();
    pull_remote_branch(repo, &rb.remote, &rb.branch, false).await
}

pub async fn pull_shallow(repo: &LocalRepository) -> Result<(), OxenError> {
    todo!()
}

pub async fn pull_all(repo: &LocalRepository) -> Result<(), OxenError> {
    todo!()
}

/// Pull a specific remote and branch
pub async fn pull_remote_branch(
    repo: &LocalRepository,
    remote: &str,
    branch: &str,
    all: bool,
) -> Result<(), OxenError> {
    println!("🐂 Oxen pull {} {}", remote, branch);

    let remote = repo
        .get_remote(&remote)
        .ok_or(OxenError::remote_not_set(&remote))?;

    let remote_data_view = match api::client::repositories::get_repo_data_by_remote(&remote).await {
        Ok(Some(repo)) => repo,
        Ok(None) => return Err(OxenError::remote_repo_not_found(&remote.url)),
        Err(err) => return Err(err),
    };

    // > 0 is a hack because only hub returns size right now, so just don't print for pure open source
    if remote_data_view.size > 0 && remote_data_view.total_files() > 0 {
        println!(
            "{} ({}) contains {} files",
            remote_data_view.name,
            bytesize::ByteSize::b(remote_data_view.size),
            remote_data_view.total_files()
        );

        println!(
            "\n  {}\n",
            RepositoryDataTypesView::data_types_str(&remote_data_view.data_types)
        );
    }

    let remote_repo = RemoteRepository::from_data_view(&remote_data_view, &remote);
    println!("🐂 remote_repo: {:?}", remote_repo);

    Ok(())
}
