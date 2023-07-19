use liboxen::api;
use liboxen::command;
use liboxen::constants;
use liboxen::core::index::CommitEntryReader;
use liboxen::error::OxenError;
use liboxen::test;
use liboxen::util;

#[test]
fn test_command_init() -> Result<(), OxenError> {
    test::run_empty_dir_test(|repo_dir| {
        // Init repo
        let repo = command::init(repo_dir)?;

        // Init should create the .oxen directory
        let hidden_dir = util::fs::oxen_hidden_dir(repo_dir);
        let config_file = util::fs::config_filepath(repo_dir);
        assert!(hidden_dir.exists());
        assert!(config_file.exists());

        // We make an initial parent commit and branch called "main"
        // just to make our lives easier down the line
        let orig_branch = api::local::branches::current_branch(&repo)?.unwrap();
        assert_eq!(orig_branch.name, constants::DEFAULT_BRANCH_NAME);
        assert!(!orig_branch.commit_id.is_empty());

        Ok(())
    })
}

#[test]
fn test_do_not_commit_any_files_on_init() -> Result<(), OxenError> {
    test::run_empty_dir_test(|dir| {
        test::populate_dir_with_training_data(dir)?;

        let repo = command::init(dir)?;
        let commits = api::local::commits::list(&repo)?;
        let commit = commits.last().unwrap();
        let reader = CommitEntryReader::new(&repo, commit)?;
        let num_entries = reader.num_entries()?;
        assert_eq!(num_entries, 0);

        Ok(())
    })
}
