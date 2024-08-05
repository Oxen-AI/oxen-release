use crate::error::OxenError;
use crate::model::commit::Commit;
use crate::model::LocalRepository;
use crate::opts::LogOpts;
use crate::repositories;

pub async fn log_commits(repo: &LocalRepository, opts: &LogOpts) -> Result<Vec<Commit>, OxenError> {
    repositories::commits::list_with_opts(repo, opts).await
}
