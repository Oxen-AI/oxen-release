use async_trait::async_trait;
use clap::{Arg, Command};
use liboxen::api;
use liboxen::core::index::commit_merkle_tree::CommitMerkleTree;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;

use crate::cmd::RunCmd;
pub const NAME: &str = "tree";
pub struct TreeCmd;

#[async_trait]
impl RunCmd for TreeCmd {
    fn name(&self) -> &str {
        NAME
    }

    fn args(&self) -> Command {
        // Setups the CLI args for the command
        Command::new(NAME)
            .about("Print the merkle tree 🌲 of a commit.")
            .arg(
                Arg::new("commit")
                    .long("commit")
                    .short('c')
                    .help("The commit to print the tree of.")
                    .default_value("HEAD")
                    .action(clap::ArgAction::Set),
            )
            .arg(
                Arg::new("depth")
                    .long("depth")
                    .short('d')
                    .help("How many levels deep to traverse the tree. -1 for all.")
                    .default_value("-1")
                    .action(clap::ArgAction::Set),
            )
    }

    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError> {
        // Parse Args
        let depth = args
            .get_one::<String>("depth")
            .expect("Must supply depth")
            .parse::<i32>()
            .expect("depth must be a valid integer.");
        let commit_id = args
            .get_one::<String>("commit")
            .expect("Must supply commit");
        let repo = LocalRepository::from_current_dir()?;

        let commit = if commit_id == "HEAD" {
            api::local::commits::head_commit(&repo)?
        } else {
            let Some(commit) = api::local::commits::get_by_id(&repo, commit_id)? else {
                return Err(OxenError::basic_str(format!("Commit {} not found", commit_id)));
            };
            commit
        };

        let tree = CommitMerkleTree::new(&repo, &commit)?;
        tree.print_depth(depth);

        Ok(())
    }
}
