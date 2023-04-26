use crate::api;
use crate::error::OxenError;
use crate::index::{CommitReader, RefReader};
use crate::model::{Commit, LocalRepository, ParsedResource};

use std::path::{Path, PathBuf};

/// Returns commit_id,branch_or_commit_id,filepath
/// Parses a path looking for either a commit id or a branch name, returns None of neither exist
pub fn parse_resource(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<(String, String, PathBuf)>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
    log::debug!("parse_resource looking for commit id in path {:?}", path);

    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        log::debug!("parse_resource looking for commit id {}", maybe_commit_id);
        if let Ok(Some(commit)) = commit_reader.get_commit_by_id(maybe_commit_id) {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            log::debug!(
                "parse_resource got commit.id [{}] and filepath [{:?}]",
                commit.id,
                file_path
            );
            return Ok(Some((commit.id.clone(), commit.id, file_path)));
        }
    }

    // See if the component has a valid branch name in it
    log::debug!("parse_resource looking for branch in path {:?}", path);
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
            "parse_resource got file path [{:?}] with {} remaining components",
            file_path,
            components.len()
        );
        // if we have no components, looking at base dir within that branch
        if components.is_empty() {
            let branch_name = file_path.to_str().unwrap();
            if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
                log::debug!(
                    "parse_resource got branch [{}] with no file path",
                    branch_name
                );

                return Ok(Some((branch.commit_id, branch.name, PathBuf::from("./"))));
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
        log::debug!("parse_resource looking for branch [{}]", branch_name);
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            log::debug!(
                "parse_resource got branch [{}] and filepath [{:?}]",
                branch_name,
                file_path
            );

            return Ok(Some((branch.commit_id, branch.name, file_path)));
        }
    }

    Ok(None)
}

pub fn parse_resource_from_path(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<ParsedResource>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
    // log::debug!("parse_resource looking for commit id in path {:?}", path);

    if let Some(first_component) = components.first() {
        let base_path: &Path = first_component.as_ref();
        let maybe_commit_id = base_path.to_str().unwrap();
        // log::debug!("parse_resource looking for commit id {}", maybe_commit_id);
        if let Some(commit) = commit_reader.get_commit_by_id(maybe_commit_id)? {
            let mut file_path = PathBuf::new();
            for (i, component) in components.iter().enumerate() {
                if i != 0 {
                    let component_path: &Path = component.as_ref();
                    file_path = file_path.join(component_path);
                }
            }
            // log::debug!(
            //     "parse_resource got commit.id [{}] and filepath [{:?}]",
            //     commit.id,
            //     file_path
            // );
            return Ok(Some(ParsedResource {
                commit,
                branch: None,
                file_path,
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

        // log::debug!(
        //     "parse_resource got file path [{:?}] with {} remaining components",
        //     file_path,
        //     components.len()
        // );
        // if we have no components, looking at base dir within that branch
        if components.is_empty() {
            let branch_name = file_path.to_str().unwrap();
            if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
                // log::debug!(
                //     "parse_resource got branch [{}] with no file path",
                //     branch_name
                // );

                let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
                return Ok(Some(ParsedResource {
                    commit,
                    branch: Some(branch),
                    file_path,
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
        // log::debug!("parse_resource looking for branch [{}]", branch_name);
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            // log::debug!(
            //     "parse_resource got branch [{}] and filepath [{:?}]",
            //     branch_name,
            //     file_path
            // );

            let commit = commit_reader.get_commit_by_id(&branch.commit_id)?.unwrap();
            return Ok(Some(ParsedResource {
                commit,
                branch: Some(branch),
                file_path,
                resource: path.to_owned(),
            }));
        }
    }

    Ok(None)
}

/// Pass in a branch name and maybe get a commit id back
pub fn maybe_get_commit_id_from_branch_name<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<String>, OxenError> {
    let ref_reader = RefReader::new(repo)?;
    ref_reader.get_commit_id_for_branch(commit_id_or_branch_name.as_ref())
}

/// Pass in a commit id or a branch name and resolve it to a
pub fn maybe_get_commit<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: S,
) -> Result<Option<Commit>, OxenError> {
    let commit_reader = CommitReader::new(repo)?;
    if let Some(commit) = commit_reader.get_commit_by_id(&commit_id_or_branch_name)? {
        return Ok(Some(commit));
    }

    match maybe_get_commit_id_from_branch_name(repo, &commit_id_or_branch_name) {
        Ok(Some(commit_id)) => commit_reader.get_commit_by_id(commit_id),
        Ok(None) => Err(OxenError::local_commit_or_branch_not_found(
            commit_id_or_branch_name.as_ref(),
        )),
        Err(err) => Err(err),
    }
}

pub fn get_commit_or_head<S: AsRef<str>>(
    repo: &LocalRepository,
    commit_id_or_branch_name: Option<S>,
) -> Result<Commit, OxenError> {
    if commit_id_or_branch_name.is_none() {
        return api::local::commits::head_commit(repo);
    }

    match maybe_get_commit(repo, commit_id_or_branch_name.unwrap().as_ref()) {
        Ok(Some(commit)) => Ok(commit),
        _ => api::local::commits::head_commit(repo),
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::api::local::resource;
    use crate::command;
    use crate::error::OxenError;

    #[test]
    fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let commit = history.first().unwrap();
            let path_str = format!("{}/annotations/train/one_shot.csv", commit.id);
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, _, path))) => {
                    assert_eq!(commit.id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a commit");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_branch() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my-branch";
            let branch = command::create_checkout_branch(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, _branch_name, path))) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_long_branch_name() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my/crazy/branch/name";
            let branch = command::create_checkout_branch(&repo, branch_name)?;

            let path_str = format!("{branch_name}/annotations/train/one_shot.csv");
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, _branch_name, path))) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.csv"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }

    #[test]
    fn test_parse_resource_for_branch_base_dir() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let branch_name = "my_branch";
            let branch = command::create_checkout_branch(&repo, branch_name)?;

            let path_str = branch_name.to_string();
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, _branch_name, path))) => {
                    println!("Got branch: {branch:?} -> {path:?}");
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("./"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }
}
