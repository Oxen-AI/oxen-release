pub mod branch;
pub mod commit;
pub mod entry;
pub mod merge_conflict;
pub mod remote;
pub mod remote_branch;
pub mod repository;
pub mod staged_data;
pub mod user;

// Repository
pub use crate::model::repository::local_repository::{LocalRepository, RepositoryNew};
pub use crate::model::repository::remote_repository::RemoteRepository;

// Commit
pub use crate::model::commit::{Commit, CommitStats, NewCommit};

// Merge
pub use crate::model::merge_conflict::MergeConflict;

// Branch
pub use crate::model::branch::Branch;
pub use crate::model::remote_branch::RemoteBranch;

// Entry
pub use crate::model::entry::commit_entry::CommitEntry;
pub use crate::model::entry::dir_entry::DirEntry;
pub use crate::model::entry::remote_entry::RemoteEntry;
pub use crate::model::entry::staged_entry::{StagedEntry, StagedEntryStatus};
pub use crate::model::entry::ContentHashable;

pub use crate::model::user::{NewUser, User};

pub use crate::model::staged_data::{StagedData, StagedDirStats};

pub use crate::model::remote::Remote;
