/// # Filenames and dirs
/// .oxen is the name of the hidden directory where all our data lives
pub const OXEN_HIDDEN_DIR: &str = ".oxen";
/// Config file for the repository
pub const REPO_CONFIG_FILENAME: &str = "config.toml";
/// HEAD file holds onto where the head commit is (commit_id or branch name)
pub const HEAD_FILE: &str = "HEAD";
/// refs/ is a key,val store of branch names to commit ids
pub const REFS_DIR: &str = "refs";
/// history/ dir is a list of directories named after commit ids
pub const HISTORY_DIR: &str = "history";
/// commits/ is a key-value database of commit ids to commit objects
pub const COMMITS_DB: &str = "commits";
/// name of the schema db
pub const SCHEMAS_DIR: &str = "schemas";
/// prefix for the commit rows
pub const ROWS_DIR: &str = "rows";
/// prefix for the commit entry files
pub const FILES_DIR: &str = "files";
/// prefix for the commit entry dirs
pub const DIRS_DIR: &str = "dirs";
/// prefix for the commit indices
pub const INDICES_DIR: &str = "indices";
/// prefix for the schema fields that are indexed
pub const FIELDS_DIR: &str = "fields";
/// versions/ is where all the versions are stored so that we can use to quickly swap between versions of the file
pub const VERSIONS_DIR: &str = "versions";
/// merge/ is where any merge conflicts are stored so that we can get rid of them
pub const MERGE_DIR: &str = "merge";

/// if we have merge conflicts we write to MERGE_HEAD and ORIG_HEAD to keep track of the parents
pub const MERGE_HEAD_FILE: &str = "MERGE_HEAD";
pub const ORIG_HEAD_FILE: &str = "ORIG_HEAD";

// Default Remotes and Origins
pub const DEFAULT_BRANCH_NAME: &str = "main";
pub const DEFAULT_REMOTE_NAME: &str = "origin";

// Namespace
pub const DEFAULT_NAMESPACE: &str = "repositories";

// Commits
pub const INITIAL_COMMIT_MSG: &str = "Initialized Repo 🐂";

// Internal Tabular Data Names
pub const ROW_NUM_COL_NAME: &str = "_row_num";
pub const ROW_HASH_COL_NAME: &str = "_row_hash";
