//! The structs and enums that are used to represent the data in the oxen library
//!

pub mod base_head;
pub mod branch;
pub mod commit;
pub mod content_type;
pub mod data_frame_size;
pub mod diff;
pub mod entry;
pub mod file;
pub mod merge_conflict;
pub mod metadata;
pub mod namespace;
pub mod object_id;
pub mod parsed_resource;
pub mod remote;
pub mod remote_branch;
pub mod repository;
pub mod schema;
pub mod staged_data;
pub mod staged_dir_stats;
pub mod summarized_staged_dir_stats;
pub mod user;

// Repository
pub use crate::model::repository::local_repository::LocalRepository;
pub use crate::model::repository::remote_repository::RemoteRepository;
pub use crate::model::repository::repo_new::RepoNew;
pub use crate::model::repository::repo_stats::{DataTypeStat, RepoStats};

// Commit
pub use crate::model::base_head::BaseHead;
pub use crate::model::commit::{Commit, CommitStats, NewCommit, NewCommitBody};

// Merge
pub use crate::model::merge_conflict::MergeConflict;

// Branch
pub use crate::model::branch::Branch;
pub use crate::model::remote_branch::RemoteBranch;

// Entry
pub use crate::model::content_type::ContentType;
pub use crate::model::diff::diff_entry::DiffEntry;
pub use crate::model::entry::commit_entry::CommitEntry;
pub use crate::model::entry::entry_data_type::EntryDataType;
pub use crate::model::entry::metadata_entry::MetadataEntry;
pub use crate::model::entry::mod_entry::ModEntry;
pub use crate::model::entry::remote_entry::RemoteEntry;
pub use crate::model::entry::staged_entry::{StagedEntry, StagedEntryStatus};
pub use crate::model::entry::ContentHashable;

// Metadata
pub use crate::model::metadata::dir_metadata_item::DirMetadataItem;

pub use crate::model::data_frame_size::DataFrameSize;

pub use crate::model::user::User;

pub use crate::model::object_id::ObjectID;
pub use crate::model::parsed_resource::ParsedResource;

pub use crate::model::staged_data::StagedData;
pub use crate::model::staged_dir_stats::StagedDirStats;
pub use crate::model::summarized_staged_dir_stats::SummarizedStagedDirStats;

pub use crate::model::remote::Remote;

pub use crate::model::diff::data_frame_diff::DataFrameDiff;

pub use crate::model::schema::staged_schema::StagedSchema;
pub use crate::model::schema::Schema;

pub use crate::model::namespace::Namespace;
