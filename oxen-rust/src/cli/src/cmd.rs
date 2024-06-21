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

pub mod commit_cache;
pub use commit_cache::CommitCacheCmd;

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

pub mod download;
pub use download::DownloadCmd;

pub mod fetch;
pub use fetch::FetchCmd;

pub mod info;
pub use info::InfoCmd;

pub mod load;
pub use load::LoadCmd;

pub mod log;
pub use log::LogCmd;

pub mod merge;
pub use merge::MergeCmd;

pub mod migrate;
pub use migrate::MigrateCmd;

pub mod moo;
pub use moo::MooCmd;

pub mod pull;
pub use pull::PullCmd;

pub mod push;
pub use push::PushCmd;

pub mod remote;

pub mod schemas;
pub use schemas::SchemasCmd;

#[async_trait]
pub trait RunCmd {
    fn name(&self) -> &str;
    fn args(&self) -> clap::Command;
    async fn run(&self, args: &clap::ArgMatches) -> Result<(), OxenError>;
}
