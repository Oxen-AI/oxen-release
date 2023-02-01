use std::path::PathBuf;

use std::path::Path;

use uuid::Uuid;

use crate::command;
use crate::error::OxenError;
use crate::model::Branch;
use crate::model::LocalRepository;
use crate::model::StagedData;
use crate::model::StagedEntry;
use crate::util;

// These methods create a directory within .oxen/staging/branch-name/ that is basically a local oxen repo
// Then we can stage data right into here using the same stager

fn branch_staging_dir(repo: &LocalRepository, branch: &Branch) -> PathBuf {
    repo.path.join(&branch.name)
}

// Stages a file in a specified directory
pub fn stage_file(
    repo: &LocalRepository,
    branch: &Branch,
    directory: &Path,
    extension: &str,
    data: &str,
) -> Result<PathBuf, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let branch_repo = if staging_dir.exists() {
        LocalRepository::new(&staging_dir)?
    } else {
        command::init(&staging_dir)?
    };

    // Write data to a temp file here, and add the file
    let uuid = Uuid::new_v4();
    let filename = format!("{}.{}", uuid, extension);
    let full_dir = staging_dir.join(directory);
    let full_path = full_dir.join(&filename);

    if !full_dir.exists() {
        std::fs::create_dir_all(&full_dir)?;
    }

    util::fs::write_to_path(&full_path, data)?;
    command::add(&branch_repo, &full_path)?;

    let relative_path = util::fs::path_relative_to_dir(&full_path, &staging_dir)?;
    Ok(relative_path)
}

// Stages a row in a DataFrame verifying the schema
pub fn stage_row(repo: &LocalRepository, branch: &Branch) -> Result<StagedEntry, OxenError> {
    Err(OxenError::basic_str("TODO"))
}

// Stages a line we want to append to a file
pub fn stage_append(repo: &LocalRepository, branch: &Branch) -> Result<StagedEntry, OxenError> {
    Err(OxenError::basic_str("TODO"))
}

pub fn list_staged_data(repo: &LocalRepository, branch: &Branch) -> Result<StagedData, OxenError> {
    let staging_dir = branch_staging_dir(repo, branch);
    let branch_repo = LocalRepository::new(&staging_dir)?;
    let status = command::status(&branch_repo)?;
    Ok(status)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::command;
    use crate::error::OxenError;
    use crate::index;
    use crate::index::remote_stager::stage_file;
    use crate::test;

    #[test]
    fn test_remote_stager_stage_file() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            // Stage file contents, pass in a directory and extension and it returns a unique file that was added
            let branch = command::current_branch(&repo)?.unwrap();
            let directory = PathBuf::from("data/");
            let extension = "md"; // markdown file
            let entry_contents = "Hello World";

            stage_file(&repo, &branch, &directory, &extension, &entry_contents)?;

            // Verify staged data
            let staged_data = index::remote_stager::list_staged_data(&repo, &branch)?;
            staged_data.print_stdout();
            assert_eq!(staged_data.added_files.len(), 1);

            Ok(())
        })
    }
}
