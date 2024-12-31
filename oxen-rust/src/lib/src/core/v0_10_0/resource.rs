use crate::core::refs::RefReader;
use crate::core::v0_10_0::index::CommitReader;

use crate::error::OxenError;
use crate::model::{LocalRepository, ParsedResource};

use std::path::{Path, PathBuf};

pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
    log::debug!(
        "parse_resource_from_path_v0_10_0 looking for commit id in path {:?}",
        path
    );

    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        // log::debug!("parse_resource looking at component {}", maybe_commit_id);
        if let Some(commit) = commit_reader.get_commit_by_id(maybe_commit_id)? {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            log::debug!(
                "parse_resource_from_path_v0_10_0 got commit.id [{}] and filepath [{:?}]",
                commit.id,
                file_path
            );
            return Ok(Some(ParsedResource {
                commit: Some(commit.clone()),
                branch: None,
                path: file_path,
                version: PathBuf::from(commit.id.to_string()),
                resource: path.to_owned(),
            }));
        }
    }

    // See if the component has a valid branch name in it
    // log::debug!("parse_resource looking for branch in path {:?}", path);
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }

        log::debug!(
            "parse_resource_from_path_v0_10_0 got file path [{:?}] with {} remaining components",
            file_path,
            components.len()
        );
        // if we have no components, looking at base dir within that branch
        if components.is_empty() {
            let branch_name = file_path.to_str().unwrap();
            if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
                // log::debug!(
                //     "parse_resource got branch [{}] with no file path",
                //     branch_name
                // );

                let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
                file_path = PathBuf::from("");
                return Ok(Some(ParsedResource {
                    commit: Some(commit),
                    branch: Some(branch.clone()),
                    path: file_path,
                    version: PathBuf::from(branch.name),
                    resource: path.to_owned(),
                }));
            } else {
                return Ok(None);
            }
        }

        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }

        let branch_name = branch_path.to_str().unwrap();
        log::debug!(
            "parse_resource_from_path_v0_10_0 looking for branch [{}]",
            branch_name
        );
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            log::debug!(
                "parse_resource_from_path_v0_10_0 got branch [{}] and filepath [{:?}]",
                branch_name,
                file_path
            );

            let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
            return Ok(Some(ParsedResource {
                commit: Some(commit),
                branch: Some(branch.clone()),
                path: file_path,
                version: PathBuf::from(branch.name),
                resource: path.to_owned(),
            }));
        }
    }

    Ok(None)
}
