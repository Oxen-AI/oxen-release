//! # oxen download
//!
//! Download a file from the remote repository
//!

use std::path::Path;
use std::sync::Arc;

use crate::api;
use crate::error::OxenError;
use crate::model::RemoteRepository;

pub async fn download(
    repo: &RemoteRepository,
    remote_path: impl AsRef<Path>,
    local_path: impl AsRef<Path>,
    committish: impl AsRef<str>,
) -> Result<(), OxenError> {
    let bar = Arc::new(indicatif::ProgressBar::new_spinner());
    api::remote::entries::download_entry(
        repo,
        remote_path.as_ref(),
        local_path.as_ref(),
        committish.as_ref(),
        &bar,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command;
    use crate::constants::DEFAULT_BRANCH_NAME;
    use crate::constants::DEFAULT_REMOTE_NAME;
    use crate::test;
    use crate::util;

    #[tokio::test]
    async fn test_download() -> Result<(), OxenError> {
        test::run_empty_remote_repo_test(|mut local_repo, remote_repo| async move {
            let cloned_remote = remote_repo.clone();
            let file_path = "hello.txt";
            let local_path = &local_repo.path.join(file_path);
            let file_contents = "Hello World";
            util::fs::write_to_path(local_path, file_contents)?;

            command::add(&local_repo, local_path)?;
            command::commit(&local_repo, "Added hello.txt")?;

            command::config::set_remote(&mut local_repo, DEFAULT_REMOTE_NAME, cloned_remote.url())?;
            command::push(&local_repo).await?;

            test::run_empty_dir_test_async(|repo_dir| async move {
                let local_path = repo_dir.join("new_name.txt");
                let committish = DEFAULT_BRANCH_NAME;

                download(&remote_repo, file_path, &local_path, committish).await?;

                assert!(local_path.exists());
                assert_eq!(util::fs::read_from_path(&local_path)?, file_contents);

                Ok(repo_dir)
            })
            .await?;

            Ok(cloned_remote)
        })
        .await?;

        Ok(())
    }
}
