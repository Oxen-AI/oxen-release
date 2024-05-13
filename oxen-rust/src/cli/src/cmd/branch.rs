
use clap::{Arg, Command};
use liboxen::error::OxenError;
use async_trait::async_trait;

use crate::cmd::RunCmd;
use crate::dispatch;

pub mod unlock;

pub const NAME: &str = "branch";

pub struct BranchCmd;

#[async_trait]
impl RunCmd for BranchCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the init command
        Command::new(NAME)
            .about("Manage branches in repository")
            .subcommand(
                unlock::BranchUnlockCmd.args()
            )
            .arg(Arg::new("name").help("Name of the branch").exclusive(true))
            .arg(
                Arg::new("all")
                    .long("all")
                    .short('a')
                    .help("List both local and remote branches")
                    .exclusive(true)
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                Arg::new("remote")
                    .long("remote")
                    .short('r')
                    .help("List all the remote branches")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("force-delete")
                    .long("force-delete")
                    .short('D')
                    .help("Force remove the local branch")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("delete")
                    .long("delete")
                    .short('d')
                    .help("Remove the local branch if it is safe to")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("move")
                    .long("move")
                    .short('m')
                    .help("Rename the current local branch.")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("show-current")
                    .long("show-current")
                    .help("Print the current branch")
                    .exclusive(true)
                    .action(clap::ArgAction::SetTrue),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        if let Some(subcommand) = args.subcommand() {
            match subcommand {
                (unlock::NAME, args) => {
                    unlock::BranchUnlockCmd.run(args).await
                }
                (cmd, _) => {
                    Err(OxenError::basic_str(format!("Unknown subcommand {cmd}")))
                }
            }
        } else if args.get_flag("all") {
            dispatch::list_all_branches().await
        } else if let Some(remote_name) = args.get_one::<String>("remote") {
            if let Some(branch_name) = args.get_one::<String>("delete") {
                dispatch::delete_remote_branch(remote_name, branch_name).await
            } else { 
                dispatch::list_remote_branches(remote_name).await
            }
        } else if let Some(name) = args.get_one::<String>("name") {
            dispatch::create_branch(name)
        } else if let Some(name) = args.get_one::<String>("delete") {
            dispatch::delete_branch(name)
        } else if let Some(name) = args.get_one::<String>("force-delete") {
            dispatch::force_delete_branch(name)
        } else if let Some(name) = args.get_one::<String>("move") {
            dispatch::rename_current_branch(name)
        } else if args.get_flag("show-current") {
            dispatch::show_current_branch()
        } else {
            dispatch::list_branches()
        }
    }
}
