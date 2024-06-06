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

pub mod config;
pub use config::ConfigCmd;

pub mod create_remote;
pub use create_remote::CreateRemoteCmd;

pub mod db;
pub use db::DbCmd;

pub mod df;
pub use df::DFCmd;

pub mod diff;
pub use diff::DiffCmd;

pub mod moo;
pub use moo::MooCmd;

pub mod log;
pub use log::LogCmd;

pub mod pack;
pub use pack::PackCmd;

pub mod remote;

pub mod schemas;
pub use schemas::SchemasCmd;

pub mod unpack;
pub use unpack::UnpackCmd;


#[async_trait]
pub trait RunCmd {
    fn name(&self) -> &str;
    fn args(&self) -> clap::Command;
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError>;
}
