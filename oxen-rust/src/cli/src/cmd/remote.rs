pub mod commit;
pub use commit::RemoteCommitCmd;

pub mod diff;
pub use diff::RemoteDiffCmd;

pub mod df;
pub use df::RemoteDfCmd;

pub mod restore;
pub use restore::RemoteRestoreCmd; 