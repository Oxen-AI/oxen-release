//! Views are the data structures that are returned by the API endpoints.
//!

pub mod branch;
pub mod commit;
pub mod compare;
pub mod entry;
pub mod entry_meta_data;
pub mod file_meta_data;
pub mod health;
pub mod http;
pub mod json_data_frame;
pub mod merge;
pub mod namespace;
pub mod oxen_response;
pub mod remote_staged_status;
pub mod repository;
pub mod schema;
pub mod status_message;
pub mod version;

pub use crate::view::compare::CompareResponse;
pub use crate::view::file_meta_data::{FileMetaData, FileMetaDataResponse, FilePathsResponse};
pub use crate::view::status_message::{
    IsValidStatusMessage, StatusMessage, StatusMessageDescription,
};

pub use crate::view::json_data_frame::{JsonDataFrame, JsonDataFrameSliceResponse};
pub use crate::view::namespace::{ListNamespacesResponse, NamespaceResponse, NamespaceView};
pub use crate::view::schema::{ListSchemaResponse, SchemaResponse};

pub use crate::view::repository::{
    ListRepositoryResponse, RepositoryResolveResponse, RepositoryResponse, RepositoryView,
};

pub use crate::view::entry::{
    EntryResponse, PaginatedDirEntries, PaginatedDirEntriesResponse, PaginatedEntries,
    RemoteEntryResponse,
};

pub use crate::view::commit::{
    CommitResponse, CommitStatsResponse, ListCommitResponse, PaginatedCommits,
};

pub use crate::view::branch::{
    BranchNew, BranchNewFromExisting, BranchResponse, BranchUpdate, ListBranchesResponse,
};

pub use crate::view::entry_meta_data::EntryMetaDataResponse;

pub use crate::view::health::HealthResponse;
pub use crate::view::oxen_response::OxenResponse;
pub use crate::view::version::VersionResponse;

pub use crate::view::remote_staged_status::{
    ListStagedFileModResponseDF, ListStagedFileModResponseRaw, RemoteStagedStatus,
    RemoteStagedStatusResponse, StagedFileModResponse,
};
