use crate::cmd::RunCmd;
use clap::{arg, Arg, Command};
use liboxen::constants::{DEFAULT_BRANCH_NAME};

use crate::cmd::add::add_args;
use crate::cmd::df::DFCmd;
use crate::cmd::remote::commit::RemoteCommitCmd;
use crate::cmd::remote::df::RemoteDfCmd;
use crate::cmd::remote::log::RemoteLogCmd;

pub const CLONE: &str = "clone";
pub const COMMIT: &str = "commit";
pub const COMPARE: &str = "compare";
pub const CONFIG: &str = "config";
pub const DOWNLOAD: &str = "download";
pub const DF: &str = "df";
pub const DIFF: &str = "diff";
pub const LOG: &str = "log";
pub const LS: &str = "ls";
pub const METADATA: &str = "metadata";
pub const READ_LINES: &str = "read-lines";
pub const REMOTE: &str = "remote";
pub const RESTORE: &str = "restore";
pub const RM: &str = "rm";
pub const SAVE: &str = "save";
pub const SCHEMAS: &str = "schemas";
pub const STATUS: &str = "status";
pub const UPLOAD: &str = "upload";

pub fn remote() -> Command {
    Command::new(REMOTE)
        .about("Interact with a remote repository without cloning everything locally.")
        // The commands that you can run locally mirrored here
        .subcommand(
            add_args()
                // can specify a path on the remote add command for where the file will be added to
                .arg(Arg::new("path")
                .long("path")
                .short('p')
                .help("Specify a path in which to add the file to. Will strip down the path to the file's basename, and add in this directory.")
                .action(clap::ArgAction::Set))
        )
        .subcommand(RemoteCommitCmd.args())
        .subcommand(RemoteDfCmd.args())
        .subcommand(RemoteLogCmd.args())
        .subcommand(diff())
        .subcommand(download())
        .subcommand(ls())
        .subcommand(restore())
        .subcommand(rm())
        .subcommand(status())
        .subcommand(metadata())
        .arg(
            Arg::new("verbose")
                .long("verbose")
                .short('v')
                .help("List the remotes that exist on this repository.")
                .action(clap::ArgAction::SetTrue),
        )
}

pub fn status() -> Command {
    Command::new(STATUS)
        .about("See at what files are ready to be added or committed")
        .arg(
            Arg::new("skip")
                .long("skip")
                .short('s')
                .help("Allows you to skip and paginate through the file list preview.")
                .default_value("0")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("Allows you to view more file list preview.")
                .default_value("10")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("print_all")
                .long("print_all")
                .short('a')
                .help("If present, does not truncate the output of status at all.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(Arg::new("path").required(false))
}

pub fn metadata() -> Command {
    Command::new(METADATA)
        .about("View computed metadata given a resource and commit")
        .subcommand(
            Command::new("list")
                .arg(Arg::new("type").required(true))
                .arg(Arg::new("path").required(false))
                .arg(
                    Arg::new("columns")
                        .long("columns")
                        .short('c')
                        .help("A comma separated set of columns names to look at. Ex file,x,y")
                        .action(clap::ArgAction::Set),
                ),
        )
        .subcommand(
            Command::new("aggregate")
                .arg(Arg::new("type").required(true))
                .arg(Arg::new("column").required(true))
                .arg(Arg::new("path").required(false)),
        )
}

pub fn ls() -> Command {
    Command::new(LS)
        .about("List the files in an oxen repo, used for remote repos you do not have locally.")
        .arg(
            Arg::new("paths")
                .default_missing_value("./")
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("Host to list from, for example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to list from, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("revision")
                .long("revision")
                .short('r')
                .help("Commit id or branch name to list from")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("page")
                .long("page")
                .help("Page number when paginating through the data frame. Default page = 1")
                .default_value("1")
                .default_missing_value("1")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("page-size")
                .long("page-size")
                .help("Paginated through the data frame. Default page-size = 10")
                .default_value("10")
                .default_missing_value("10")
                .action(clap::ArgAction::Set),
        )
}

pub fn schemas() -> Command {
    Command::new(SCHEMAS)
        .about("Manage schemas that are created from committing tabular data")
        .arg(arg!([SCHEMA_REF] "Name, hash, or path of the schema you want to view in more detail."))
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Show the staged schema")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("flatten")
                .long("flatten")
                .help("Print the schema in a flattened format")
                .action(clap::ArgAction::SetTrue),
        )
        .subcommand(
            Command::new("list")
                .about("List the committed schemas.")
                .arg(
                    Arg::new("staged")
                        .long("staged")
                        .help("List the staged schemas")
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("name")
                .about("Name a schema by hash.")
                .arg(Arg::new("HASH").help("Hash of the schema you want to name."))
                .arg(Arg::new("NAME").help("Name of the schema.")),
        )
        .subcommand(
            Command::new("add")
                .about("Apply a schema on read to a data frame")
                .arg(Arg::new("PATH").help("The path of the data frame file."))
                .arg(
                    Arg::new("column")
                        .long("column")
                        .short('c')
                        .help("The column that you want to override the data type or metadata for.")
                )
                .arg(
                    Arg::new("metadata")
                        .long("metadata")
                        .short('m')
                        .help("Set the metadata for a specific column. Must pass in the -c flag.")
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a schema from the list of committed or added schemas.")
                .arg(arg!(<NAME_OR_HASH> ... "Name, hash, or path of the schema you want to remove."))
                .arg(
                    Arg::new("staged")
                        .long("staged")
                        .help("Removed a staged schema")
                        .action(clap::ArgAction::SetTrue),
                ),
        )
        .subcommand(
            Command::new("metadata")
                .about("Add additional metadata to a schema.")
                .arg(Arg::new("PATH").help("The path of the data frame file."))
                .arg(Arg::new("METADATA").help("Any additional metadata you want to add to the schema."))

        )
        .subcommand(DFCmd.args())
}

pub fn download() -> Command {
    Command::new(DOWNLOAD)
        .about("Download a specific file from the remote repository")
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("output")
                .long("output")
                .short('o')
                .help("Output file to store the downloaded data")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("Host to download from, for example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to download from, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("revision")
                .long("revision")
                .help("The branch or commit id to download the data from. Defaults to main branch. If a branch is specified, it will download the latest commit from that branch.")
                .action(clap::ArgAction::Set),
        )
}

pub fn upload() -> Command {
    Command::new(UPLOAD)
        .about("Upload a specific file to the remote repository.")
        .arg(
            Arg::new("paths")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("dst")
                .long("destination")
                .short('d')
                .help("The destination directory to upload the data to. Defaults to the root './' of the repository.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("branch")
                .long("branch")
                .short('b')
                .help("The branch to upload the data to. Defaults to main branch.")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("message")
                .help("The message for the commit. Should be descriptive about what changed.")
                .long("message")
                .short('m')
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("host")
                .long("host")
                .help("Host to upload the data to, for example: 'hub.oxen.ai'")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("remote")
                .long("remote")
                .help("Remote to up the data to, for example: 'origin'")
                .action(clap::ArgAction::Set),
        )
}

pub fn rm() -> Command {
    Command::new(RM)
        .about("Removes the specified files from the index")
        .arg(
            Arg::new("files")
                .required(true)
                .action(clap::ArgAction::Append),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Removes the file from the staging area.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("recursive")
                .long("recursive")
                .short('r')
                .help("Recursively removes directory.")
                .action(clap::ArgAction::SetTrue),
        )
}

pub fn restore() -> Command {
    Command::new(RESTORE)
        .about("Restore specified paths in the working tree with some contents from a restore source.")
        .arg(arg!(<PATH> ... "The files or directory to restore"))
        .arg_required_else_help(true)
        .arg(
            Arg::new("source")
                .long("source")
                .help("Restores a specific revision of the file. Can supply commit id or branch name")
                .action(clap::ArgAction::Set),
        )
        .arg(
            Arg::new("staged")
                .long("staged")
                .help("Restore content in staging area. By default, if --staged is given, the contents are restored from HEAD. Use --source to restore from a different commit.")
                .action(clap::ArgAction::SetTrue),
        )
}

pub fn clone() -> Command {
    Command::new(CLONE)
        .about("Clone a repository by its URL")
        .arg_required_else_help(true)
        .arg(arg!(<URL> "URL of the repository you want to clone"))
        .arg(
            Arg::new("shallow")
                .long("shallow")
                .help("A shallow clone doesn't actually clone the data files. Useful if you want to soley interact with the remote.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("all")
                .long("all")
                .help("This downloads the full commit history, all the data files, and all the commit databases. Useful if you want to have the entire history locally or push to a new remote.")
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            Arg::new("branch")
                .long("branch")
                .short('b')
                .help("The branch you want to pull to when you clone.")
                .default_value(DEFAULT_BRANCH_NAME)
                .default_missing_value(DEFAULT_BRANCH_NAME)
                .action(clap::ArgAction::Set),
        )
}

pub fn diff() -> Command {
    Command::new(DIFF)
        .about("Compare two files against each other or against versions. The two resource paramaters can be specified by filepath or `file:revision` syntax.")
        .arg(Arg::new("RESOURCE1")
            .required(true)
            .help("First resource, in format `file` or `file:revision`")
            .index(1)
        )
        .arg(Arg::new("RESOURCE2")
            .required(false)
            .help("Second resource, in format `file` or `file:revision`. If left blank, RESOURCE1 will be compared with HEAD.")
            .index(2))
        .arg(Arg::new("keys")
            .required(false)
            .long("keys")
            .short('k')
            .help("Comma-separated list of columns to compare on. If not specified, all columns are used for keys.")
            .use_value_delimiter(true)
            .action(clap::ArgAction::Set))
        .arg(Arg::new("compares")
            .required(false)
            .long("compares")
            .short('c')
            .help("Comma-separated list of columns to compare changes between. If not specified, all columns  that are not keys are compares.")
            .use_value_delimiter(true)
            .action(clap::ArgAction::Set))
        .arg(Arg::new("output")
            .required(false)
            .long("output")
            .short('o')
            .help("Output directory path to write the results of the comparison. Will write both match.csv (rows with same keys and compares) and diff.csv (rows with different compares between files.")
            .action(clap::ArgAction::Set))
}

pub fn save() -> Command {
    Command::new(SAVE)
        .arg(
            Arg::new("PATH")
                .help("Path of the local repository to save")
                .required(true)
                .index(1), // This represents the position of the argument in the command line command.
        )
        .arg(
            Arg::new("output")
                .help("Name of the output .tar.gz archive")
                .short('o')
                .long("output")
                .required(true),
        )
}
