use clap::{arg, Arg, Command};

pub const INIT: &str = "init";
pub const CONFIG: &str = "config";
pub const CREATE_REMOTE: &str = "create-remote";
pub const REMOTE: &str = "remote";
pub const STATUS: &str = "status";
pub const LOG: &str = "log";
pub const DF: &str = "df";
pub const SCHEMAS: &str = "schemas";
pub const ADD: &str = "add";
pub const RM: &str = "rm";
pub const COMMIT: &str = "commit";
pub const RESTORE: &str = "restore";
pub const BRANCH: &str = "branch";
pub const CHECKOUT: &str = "checkout";
pub const MERGE: &str = "merge";
pub const CLONE: &str = "clone";
pub const PUSH: &str = "push";
pub const PULL: &str = "pull";
pub const DIFF: &str = "diff";
pub const MIGRATE: &str = "migrate";
pub const LS: &str = "ls";
pub const KVDB_INSPECT: &str = "kvdb-inspect";
pub const READ_LINES: &str = "read-lines";

pub fn init() -> Command<'static> {
    Command::new(INIT)
        .about("Initializes a local repository")
        .arg(arg!([PATH] "The directory to establish the repo in. Defaults to the current directory."))
}

pub fn config() -> Command<'static> {
    Command::new(CONFIG)
        .about("Sets the user configuration in ~/.oxen/user_config.toml")
        .arg(
            Arg::new("name")
                .long("name")
                .short('n')
                .help("Set the name you want your commits to be saved as.")
                .takes_value(true),
        )
        .arg(
            Arg::new("email")
                .long("email")
                .short('e')
                .help("Set the email you want your commits to be saved as.")
                .takes_value(true),
        )
        // Note: we differ from git here because we have the concept of a remote
        //       staging area which uses the `oxen remote add` subcommand
        .arg(
            Arg::new("set-remote")
                .long("set-remote")
                .number_of_values(2)
                .value_names(&["NAME", "URL"])
                .help("Set a remote for your current working repository.")
                .takes_value(true),
        )
        // "delete-remote" is easier to read than "remove-remote"
        .arg(
            Arg::new("delete-remote")
                .long("delete-remote")
                .number_of_values(2)
                .help("Delete a remote from the current working repository.")
                .takes_value(true),
        )
        .arg(
            Arg::new("auth-token")
                .long("auth")
                .short('a')
                .number_of_values(2)
                .value_names(&["HOST", "TOKEN"])
                .help("Set the authentication token for a specific oxen-server host.")
                .takes_value(true),
        )
        .arg(
            Arg::new("default-host")
                .long("default-host")
                .help("Sets the default host used to check version numbers. If empty, the CLI will not do a version check.")
                .takes_value(true),
        )
}

pub fn create_remote() -> Command<'static> {
    Command::new(CREATE_REMOTE)
        .about("Creates a remote repository with the name on the host")
        .arg(arg!(<NAMESPACE> "The namespace you would like to use"))
        .arg(arg!(<NAME> "The remote host"))
        .arg(arg!(<HOST> "The remote host"))
        .arg_required_else_help(true)
}

pub fn remote() -> Command<'static> {
    Command::new(REMOTE)
        .about("Interact with a remote repository without cloning everything locally.")
        // The commands that you can run locally mirrored here
        .subcommand(status())
        .subcommand(
            add()
                // can specify a path on the remote add command for where the file will be added to
                .arg(Arg::new("path")
                .long("path")
                .short('p')
                .help("Specify a path in which to add the file to. Will strip down the path to the file's basename, and add in this directory.")
                .takes_value(true))
        )
        .subcommand(rm())
        .subcommand(restore())
        .subcommand(commit())
        .subcommand(log())
        .subcommand(df())
        .subcommand(diff())
        .subcommand(ls())
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .help("List the remotes that exist on this repository.")
                .takes_value(false),
        )
}

pub fn status() -> Command<'static> {
    Command::new(STATUS)
        .about("See at what files are ready to be added or committed")
        .arg(
            Arg::new("skip")
                .long("skip")
                .short('s')
                .help("Allows you to skip and paginate through the file list preview.")
                .default_value("0")
                .takes_value(true),
        )
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("Allows you to view more file list preview.")
                .default_value("10")
                .takes_value(true),
        )
        .arg(
            Arg::new("print_all")
                .long("print_all")
                .short('a')
                .help("If present, does not truncate the output of status at all.")
                .takes_value(false),
        )
        .arg(Arg::new("path").required(false))
}

pub fn log() -> Command<'static> {
    Command::new(LOG)
        .about("See log of commits")
        .arg(arg!([COMMITTISH] "The commit or branch id you want to get history from. Defaults to main."))
}

pub fn ls() -> Command<'static> {
    Command::new(LS)
        .about("List the files in an oxen repo, used for remote repos you do not have locally.")
        .arg(arg!([PATH] "The path you want to list."))
        .arg(
            Arg::new("page")
                .long("page")
                .help("Page number when paginating through the data frame. Default page = 1")
                .takes_value(true),
        )
        .arg(
            Arg::new("page-size")
                .long("page-size")
                .help("Paginated through the data frame. Default page-size = 10")
                .takes_value(true),
        )
}

pub fn df() -> Command<'static> {
    Command::new(DF)
        .about("View and transform data frames. Supported types: csv, tsv, ndjson, jsonl, parquet.")
        .arg(arg!(<DF_SPEC> ... "The DataFrame you want to process. If in the schema subcommand the schema ref."))
        .arg_required_else_help(true)
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the transformed data")
                .takes_value(true),
        )
        .arg(
            Arg::new("delimiter")
                .long("delimiter")
                .short('d')
                .help("The delimiter to use when reading the file. Default is ','")
                .takes_value(true),
        )
        .arg(
            Arg::new("columns")
                .long("columns")
                .short('c')
                .help("A comma separated set of columns names to look at. Ex file,x,y")
                .takes_value(true),
        )
        .arg(
            Arg::new("filter")
                .long("filter")
                .short('f')
                .help("An filter the row data based on an expression. Supported Ops (=, !=, >, <, <= , >=) Supported dtypes (str,int,float)")
                .takes_value(true),
        )
        .arg(
            Arg::new("aggregate")
                .long("aggregate")
                .short('a')
                .help("Aggregate up values based on field.")
                .takes_value(true),
        )
        .arg(
            Arg::new("col-at")
                .long("col-at")
                .help("Select a specific row item from column to view it fully. Format: 'col_name:index' ie: 'my_col_name:3'")
                .takes_value(true),
        )
        .arg(
            Arg::new("vstack")
                .long("vstack")
                .help("Combine row data from different files. The number of columns must match.")
                .takes_value(true)
                .multiple_values(true),
        )
        .arg(
            Arg::new("slice")
                .long("slice")
                .help("A continuous slice of the data you want to look at. Format: 'start..end' Ex) '10..25' will take 15 elements, starting at 10 and ending at 25.")
                .takes_value(true),
        )
        .arg(
            Arg::new("head")
                .long("head")
                .help("Grab the first N entries of the data frame.")
                .takes_value(true),
        )
        .arg(
            Arg::new("tail")
                .long("tail")
                .help("Grab the last N entries of the data frame.")
                .takes_value(true),
        )
        .arg(
            Arg::new("page")
                .long("page")
                .help("Page number when paginating through the data frame. Default page = 1")
                .takes_value(true),
        )
        .arg(
            Arg::new("page-size")
                .long("page-size")
                .help("Paginated through the data frame. Default page-size = 10")
                .takes_value(true),
        )
        .arg(
            Arg::new("take")
                .long("take")
                .short('t')
                .help("A comma separated set of row indices to look at. Ex 1,22,313")
                .takes_value(true),
        )
        .arg(
            Arg::new("add-col")
                .long("add-col")
                .help("Add a column with a default value to the data table. If used with --add-row, row is added first, then column. Format 'name:val:dtype'")
                .takes_value(true),
        )
        .arg(
            Arg::new("add-row")
                .long("add-row")
                .help("Add a row and cast to the values data types to match the current schema. If used with --add-col, row is added first, then column. Format 'comma,separated,vals'")
                .takes_value(true),
        )
        .arg(
            Arg::new("delete-row")
                .long("delete-row")
                .help("Delete a row from a data frame. Currently only works with remote data frames with the value from _id column.")
                .takes_value(true),
        )
        .arg(
            Arg::new("content-type")
                .long("content-type")
                .help("The data that you want to append to the end of the file. Valid content types are 'json', 'csv', 'text'.")
                .takes_value(true),
        )
        .arg(
            Arg::new("sort")
                .long("sort")
                .short('s')
                .help("Sort the output by a column name. Is run at the end of all the other transforms.")
                .takes_value(true),
        )
        .arg(
            Arg::new("unique")
                .long("unique")
                .short('u')
                .help("Unique the output by a set of column names. Takes a comma separated set of column names ie: \"text,label\".")
                .takes_value(true),
        )
        .arg(
            Arg::new("randomize")
                .long("randomize")
                .help("Randomize the order of the table"),
        )
        .arg(
            Arg::new("reverse")
                .long("reverse")
                .help("Reverse the order of the table"),
        )
        .arg(
            Arg::new("schema")
                .long("schema")
                .help("Print the full list of columns and data types within the schema in a dataframe."),
        )
        .arg(
            Arg::new("schema_flat")
                .long("schema_flat")
                .help("Print the full list of columns and data types within the schema."),
        )
}

pub fn schemas() -> Command<'static> {
    Command::new(SCHEMAS)
        .about("Manage schemas that are created from committing tabular data")
        .subcommand(
            Command::new("list").arg(
                Arg::new("staged")
                    .long("staged")
                    .help("List the staged schemas"),
            ),
        )
        .subcommand(
            Command::new("show")
                .arg(arg!(<NAME_OR_HASH> ... "Name or the hash of the schema you want to view."))
                .arg(
                    Arg::new("staged")
                        .long("staged")
                        .help("Show the staged schema"),
                ),
        )
        .subcommand(
            Command::new("name")
                .arg(Arg::new("HASH").help("Hash of the schema you want to name."))
                .arg(Arg::new("NAME").help("Name of the schema.")),
        )
        .subcommand(df())
}

pub fn add() -> Command<'static> {
    Command::new(ADD)
        .about("Adds the specified files or directories")
        .arg(Arg::new("files").required(true).min_values(1))
}

pub fn commit() -> Command<'static> {
    Command::new(COMMIT)
        .about("Commit the staged files to the repository")
        .arg(
            Arg::new("message")
                .help("Use the given <message> as the commit message.")
                .long("message")
                .short('m')
                .required(true)
                .takes_value(true),
        )
}

pub fn rm() -> Command<'static> {
    Command::new(RM)
        .about("Removes the specified files from the index")
        .arg(Arg::new("files").required(true).min_values(1))
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Removes the file from the staging area."),
        )
        .arg(
            Arg::new("recursive")
                .long("recursive")
                .short('r')
                .help("Recursively removes directory."),
        )
}

pub fn restore() -> Command<'static> {
    Command::new(RESTORE)
        .about("Restore specified paths in the working tree with some contents from a restore source.")
        .arg(arg!(<PATH> ... "The files or directory to restore"))
        .arg_required_else_help(true)
        .arg(
            Arg::new("source")
                .long("source")
                .help("Restores a specific revision of the file. Can supply commit id or branch name")
                .takes_value(true),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Restore content in staging area. By default, if --staged is given, the contents are restored from HEAD. Use --source to restore from a different commit.")
        )
}

pub fn branch() -> Command<'static> {
    Command::new(BRANCH)
        .about("Manage branches in repository")
        .arg(Arg::new("name").help("Name of the branch").exclusive(true))
        .arg(
            Arg::new("all")
                .long("all")
                .short('a')
                .help("List both local and remote branches")
                .exclusive(true)
                .takes_value(false),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .short('r')
                .help("List all the remote branches")
                .takes_value(true),
        )
        .arg(
            Arg::new("force-delete")
                .long("force-delete")
                .short('D')
                .help("Force remove the local branch")
                .takes_value(true),
        )
        .arg(
            Arg::new("delete")
                .long("delete")
                .short('d')
                .help("Remove the local branch if it is safe to")
                .takes_value(true),
        )
        .arg(
            Arg::new("move")
                .long("move")
                .short('m')
                .help("Rename the current local branch.")
                .takes_value(true),
        )
        .arg(
            Arg::new("show-current")
                .long("show-current")
                .help("Print the current branch")
                .exclusive(true)
                .takes_value(false),
        )
}

pub fn checkout() -> Command<'static> {
    Command::new(CHECKOUT)
        .about("Checks out a branches in the repository")
        .arg(Arg::new("name").help("Name of the branch or commit id to checkout"))
        .arg(
            Arg::new("create")
                .long("create")
                .short('b')
                .help("Create the branch and check it out")
                .exclusive(true)
                .takes_value(true),
        )
        .arg(
            Arg::new("theirs")
                .long("theirs")
                .help("Checkout the content of the conflicting branch and take it as your data. Will overwrite your working file.")
                .takes_value(false),
        )
}

pub fn merge() -> Command<'static> {
    Command::new(MERGE)
        .about("Merges a branch into the current checked out branch.")
        .arg_required_else_help(true)
        .arg(arg!(<BRANCH> "The name of the branch you want to merge in."))
}

pub fn clone() -> Command<'static> {
    Command::new(CLONE)
        .about("Clone a repository by its URL")
        .arg_required_else_help(true)
        .arg(arg!(<URL> "URL of the repository you want to clone"))
        .arg(
            Arg::new("shallow")
                .long("shallow")
                .help("A shallow clone doesn't actually clone the data files, useful if you want to pull a specific branch instead.")
                .takes_value(false),
        )
        .arg(
            Arg::new("branch")
                .long("branch")
                .short('b')
                .help("The branch you want to switch to when you clone.")
                .takes_value(true),
        )
}

pub fn inspect_kv_db() -> Command<'static> {
    Command::new(KVDB_INSPECT)
        .about("Inspect a key-val pair db. For debugging purposes.")
        .arg_required_else_help(true)
        .arg(arg!(<PATH> "The path to the database you want to inspect"))
}

pub fn push() -> Command<'static> {
    Command::new(PUSH)
        .about("Push the the files to the remote branch")
        .arg(arg!(<REMOTE> "Remote you want to pull from"))
        .arg(
            Arg::new("delete")
                .long("delete")
                .short('d')
                .help("Remove the remote branch")
                .takes_value(false),
        )
        .arg(arg!(<BRANCH> "Branch name to pull"))
}

pub fn pull() -> Command<'static> {
    Command::new(PULL)
        .about("Pull the files up from a remote branch")
        .arg(arg!(<REMOTE> "Remote you want to pull from"))
        .arg(arg!(<BRANCH> "Branch name to pull"))
}

pub fn diff() -> Command<'static> {
    Command::new(DIFF)
        .about("Compare two files against each other or against versions. The first parameter can be one of three things 1) another file 2) a commit hash 3) a branch name. If the first parameter is a committish it will compare the second parameter path to that version of the file.")
        .arg(Arg::new("FILE_OR_COMMITTISH").required(true))
        .arg(Arg::new("PATH").required(false))
}

pub fn migrate() -> Command<'static> {
    Command::new(MIGRATE)
        .about("Migrate a repository or set of repositories")
        .arg(Arg::new("PATH").required(true))
        .arg(
            Arg::new("all")
                .long("all")
                .short('a')
                .help("Migrate all the oxen repositories in this directory")
                .takes_value(false),
        )
        .arg(arg!([COMMITTISH] "The commit or branch id you want to get history from. Defaults to main."))
}

pub fn read_lines() -> Command<'static> {
    Command::new("read-lines")
        .about("Read a set of lines from a file without loading it all into memory")
        .arg(arg!(<PATH> "Path to file you want to read"))
        .arg(arg!(<START> "Start index of file"))
        .arg(arg!(<LENGTH> "Length you want to read"))
}
