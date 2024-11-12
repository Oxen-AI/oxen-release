use crate::error::OxenError;
use std::fs;
use std::path::Path;

pub fn fork(original_repo_path: &Path, new_repo_path: &Path) -> Result<(), OxenError> {
    // Check if a repository already exists at the destination path
    if new_repo_path.exists() {
        return Err(OxenError::basic_str(
            "A file already exists at the destination path.",
        ));
    }

    // Create the destination directory if it doesn't exist
    fs::create_dir_all(new_repo_path)?;

    // Perform the manual copy
    copy_dir_recursive(original_repo_path, new_repo_path)?;

    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), OxenError> {
    // Iterate over the entries in the source directory
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = dst.join(entry.file_name());

        // Skip the .oxen/workspaces directory
        if path.ends_with(".oxen/workspaces") {
            continue;
        }

        if path.is_dir() {
            // Create the directory in the destination
            fs::create_dir_all(&dest_path)?;
            // Recursively copy the directory
            copy_dir_recursive(&path, &dest_path)?;
        } else {
            // Copy the file to the destination
            fs::copy(&path, &dest_path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OxenError;
    use crate::test;
    use std::path::Path;

    #[test]
    fn test_fork_creates_new_repo() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|original_repo| {
            let original_repo_path = &original_repo.path;

            let new_repo_path = original_repo_path.parent().unwrap().join("forked/new_repo");
            if new_repo_path.exists() {
                std::fs::remove_dir_all(&new_repo_path)?;
            }
            fork(original_repo_path, &new_repo_path)?;
            let config_path = original_repo_path.join(".oxen/config.toml");

            assert!(new_repo_path.exists());
            // Verify that the content of .oxen/config.toml is the same in both repos
            let new_config_path = new_repo_path.join(".oxen/config.toml");
            let original_content = fs::read_to_string(&config_path)?;
            let new_content = fs::read_to_string(&new_config_path)?;

            assert_eq!(
                original_content, new_content,
                "The content of .oxen/config.toml should be the same in both repositories"
            );

            Ok(())
        })
    }

    #[test]
    fn test_fork_fails_if_repo_exists() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|original_repo| {
            let original_repo_path = &original_repo.path;
            let new_repo_path = original_repo_path
                .parent()
                .unwrap()
                .join("forked/new_repo_1");
            if new_repo_path.exists() {
                std::fs::remove_dir_all(&new_repo_path)?;
            }
            std::fs::create_dir_all(&new_repo_path)?;

            let result = fork(original_repo_path, &new_repo_path);

            assert!(result.is_err());

            Ok(())
        })
    }

    #[test]
    fn test_fork_excludes_workspaces() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|original_repo| {
            // Define paths
            let original_repo_path = &original_repo.path;
            let new_repo_path = original_repo_path
                .parent()
                .unwrap()
                .join("forked/new_repo_2");

            let workspaces_path = original_repo_path.join(".oxen/workspaces");
            // Create a workspace directory and add a file to it
            std::fs::create_dir_all(&workspaces_path)?;
            let workspace_file = workspaces_path.join("test_workspace.txt");
            std::fs::write(workspace_file, "test workspace content")?;

            fork(original_repo_path, &new_repo_path)?;

            // Check that the new repository exists
            assert!(new_repo_path.exists());

            // Verify that .oxen/workspaces was not copied
            let new_workspaces_path = new_repo_path.join(".oxen/workspaces");
            assert!(
                !new_workspaces_path.exists(),
                ".oxen/workspaces should not be copied"
            );

            Ok(())
        })
    }
}
