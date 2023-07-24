use crate::api;
use crate::constants::DEFAULT_BRANCH_NAME;
use crate::error::OxenError;
use crate::model::RemoteRepository;

/// Looks at branch or commit id and resolves to commit id. Falls back to main branch.
pub async fn remote_commit_id(
    repo: &RemoteRepository,
    commit_id: &Option<String>,
    branch_name: &Option<String>,
) -> Result<String, OxenError> {
    // Commit id has highest priority
    if let Some(commit_id) = commit_id {
        if api::remote::commits::get_by_id(repo, commit_id)
            .await?
            .is_some()
        {
            return Ok(commit_id.to_string());
        }
    }

    // Then see if the branch exists
    if let Some(branch_name) = branch_name {
        if let Some(branch) = api::remote::branches::get_by_name(repo, branch_name).await? {
            return Ok(branch.commit_id.to_string());
        }
    }

    // Fallback to main branch
    let main_branch = api::remote::branches::get_by_name(repo, DEFAULT_BRANCH_NAME).await?;
    if main_branch.is_none() {
        return Err(OxenError::basic_str("No main branch found on remote."));
    }
    Ok(main_branch.unwrap().commit_id.to_string())
}
