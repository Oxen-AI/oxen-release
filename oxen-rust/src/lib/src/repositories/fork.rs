use crate::error::OxenError;
use std::fs;
use std::path::Path;

pub fn fork(original_repo_path: &Path, new_repo_path: &Path) -> Result<(), OxenError> {
    if new_repo_path.exists() {
        return Err(OxenError::basic_str(
            "A file already exists at the destination path.",
        ));
    }

    fs::create_dir_all(new_repo_path)?;

    copy_dir_recursive(original_repo_path, new_repo_path)?;

    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<(), OxenError> {
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = dst.join(entry.file_name());

        if path.ends_with(".oxen/workspaces") {
            continue;
        }

        if path.is_dir() {
            fs::create_dir_all(&dest_path)?;
            copy_dir_recursive(&path, &dest_path)?;
        } else {
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

    #[test]
    fn test_fork_operations() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|original_repo| {
            let original_repo_path = &original_repo.path;

            // Fork creates new repo
            let new_repo_path = original_repo_path.parent().unwrap().join("forked/new_repo");
            if new_repo_path.exists() {
                std::fs::remove_dir_all(&new_repo_path)?;
            }

            let dir_path = original_repo_path.join("dir");
            // Create a workspace directory and add a file to it
            std::fs::create_dir_all(&dir_path)?;
            let file_path = dir_path.join("test_file.txt");
            std::fs::write(file_path, "test file content")?;

            fork(original_repo_path, &new_repo_path)?;
            let file_path = original_repo_path.join("dir/test_file.txt");

            assert!(new_repo_path.exists());
            // Verify that the content of .oxen/config.toml is the same in both repos
            let new_file_path = new_repo_path.join("dir/test_file.txt");
            let original_content = fs::read_to_string(&file_path)?;
            let new_content = fs::read_to_string(&new_file_path)?;

            assert_eq!(
                original_content, new_content,
                "The content of test_file.txt should be the same in both repositories"
            );

            // Fork fails if repo exists
            let new_repo_path_1 = original_repo_path
                .parent()
                .unwrap()
                .join("forked/new_repo_1");
            if new_repo_path_1.exists() {
                std::fs::remove_dir_all(&new_repo_path_1)?;
            }
            std::fs::create_dir_all(&new_repo_path_1)?;

            let result = fork(original_repo_path, &new_repo_path_1);
            assert!(
                result.is_err(),
                "Expected an error because the repo already exists."
            );

            // Fork excludes workspaces
            let new_repo_path_2 = original_repo_path
                .parent()
                .unwrap()
                .join("forked/new_repo_2");

            let workspaces_path = original_repo_path.join(".oxen/workspaces");
            // Create a workspace directory and add a file to it
            std::fs::create_dir_all(&workspaces_path)?;
            let workspace_file = workspaces_path.join("test_workspace.txt");
            std::fs::write(workspace_file, "test workspace content")?;

            fork(original_repo_path, &new_repo_path_2)?;

            // Check that the new repository exists
            assert!(new_repo_path_2.exists());

            // Verify that .oxen/workspaces was not copied
            let new_workspaces_path = new_repo_path_2.join(".oxen/workspaces");
            assert!(
                !new_workspaces_path.exists(),
                ".oxen/workspaces should not be copied"
            );

            Ok(())
        })
    }
}
