use clap;
use liboxen::error::OxenError;

use async_trait::async_trait;

pub mod init;
pub use init::InitCmd;

pub mod add;
pub use add::AddCmd;

pub mod branch;
pub use branch::BranchCmd;

pub mod checkout;
pub use checkout::CheckoutCmd;

pub mod clone;
pub use clone::CloneCmd;

pub mod commit;
pub use commit::CommitCmd;

pub mod remote;

pub mod moo;
pub use moo::MooCmd;

#[async_trait]
pub trait RunCmd {
    fn name(&self) -> &str;
    fn args(&self) -> clap::Command;
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError>;
}
