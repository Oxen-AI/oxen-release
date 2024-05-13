
use clap;
use liboxen::error::OxenError;

use async_trait::async_trait;

pub mod init;
pub use init::InitCmd;

pub mod add;
pub use add::AddCmd;

pub mod branch;
pub use branch::BranchCmd;

pub mod moo;
pub use moo::MooCmd;

#[async_trait]
pub trait RunCmd {
    fn name(&self) -> &str;
    fn args(&self) -> clap::Command;
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError>;
}