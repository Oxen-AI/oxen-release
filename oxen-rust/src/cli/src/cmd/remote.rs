pub mod commit;
pub use commit::RemoteCommitCmd;

pub mod diff;
pub use diff::RemoteDiffCmd;

pub mod df;
pub use df::RemoteDfCmd;

pub mod restore;
pub use restore::RemoteRestoreCmd;

pub mod log;
pub use log::RemoteLogCmd;

pub mod rm;
pub use rm::RemoteRmCmd;
