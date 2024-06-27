use crate::api;
use crate::error::OxenError;
use crate::model::commit::Commit;
use crate::model::LocalRepository;
use crate::opts::LogOpts;

pub async fn log_commits(repo: &LocalRepository, opts: &LogOpts) -> Result<Vec<Commit>, OxenError> {
    api::local::commits::list_with_opts(repo, opts).await
}
