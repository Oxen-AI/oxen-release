use crate::error::OxenError;
use crate::index::{CommitReader, RefReader};
use crate::model::LocalRepository;

use std::path::{Path, PathBuf};

/// Returns commit_id,filepath
/// Parses a path looking for either a commit id or a branch name, returns None of neither exist
pub fn parse_resource(
    repo: &LocalRepository,
    path: &Path,
) -> Result<Option<(String, PathBuf)>, OxenError> {
    let mut components = path.components().collect::<Vec<_>>();
    let commit_reader = CommitReader::new(repo)?;

    // See if the first component is the commit id
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
            return Ok(Some((commit.id, file_path)));
        }
    }

    // See if the component has a valid branch name in it
    let ref_reader = RefReader::new(repo)?;
    let mut file_path = PathBuf::new();
    while let Some(component) = components.pop() {
        let component_path: &Path = component.as_ref();
        if file_path == PathBuf::new() {
            file_path = component_path.to_path_buf();
        } else {
            file_path = component_path.join(file_path);
        }

        let mut branch_path = PathBuf::new();
        for component in components.iter() {
            let component_path: &Path = component.as_ref();
            branch_path = branch_path.join(component_path);
        }

        let branch_name = branch_path.to_str().unwrap();
        log::debug!("parse_resource looking for branch {}", branch_name);
        if let Some(branch) = ref_reader.get_branch_by_name(branch_name)? {
            return Ok(Some((branch.commit_id, file_path)));
        }
    }

    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use crate::command;
    use crate::error::OxenError;
    use crate::util::resource;

    #[test]
    fn test_parse_resource_for_commit() -> Result<(), OxenError> {
        crate::test::run_training_data_repo_test_fully_committed(|repo| {
            let history = command::log(&repo)?;
            let commit = history.first().unwrap();
            let path_str = format!("{}/annotations/train/one_shot.txt", commit.id);
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    assert_eq!(commit.id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
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

            let path_str = format!("{}/annotations/train/one_shot.txt", branch_name);
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    println!("Got branch: {:?} -> {:?}", branch, path);
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
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

            let path_str = format!("{}/annotations/train/one_shot.txt", branch_name);
            let path = Path::new(&path_str);

            match resource::parse_resource(&repo, path) {
                Ok(Some((commit_id, path))) => {
                    println!("Got branch: {:?} -> {:?}", branch, path);
                    assert_eq!(branch.commit_id, commit_id);
                    assert_eq!(path, Path::new("annotations/train/one_shot.txt"));
                }
                _ => {
                    panic!("Should return a branch");
                }
            }

            Ok(())
        })
    }
}
