use std::{collections::HashMap, path::PathBuf, time::Duration};

use super::StatusMessage;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionFile {
    pub hash: String,
    pub size: u64,
}

#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionFileResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub version: VersionFile,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompletedFileUpload {
    pub hash: String,
    pub file_name: String,        // The name of the file
    pub dst_dir: Option<PathBuf>, // The destination directory for the file
    // Number of chunks that were uploaded for this file (new clients send this)
    #[serde(default)]
    pub num_chunks: Option<usize>,
    // Deprecated: old clients send this list of chunk upload headers
    #[serde(default)]
    pub upload_results: Option<Vec<HashMap<String, String>>>,
}

/// Body of `POST /versions/uploads`: the file's hash and size, and where to stage it once it
/// completes.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct BeginVersionUploadRequest {
    pub hash: String,
    pub size: u64,
    pub destination: Option<WorkspaceDestination>,
}

/// The workspace and path to stage an uploaded file at.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct WorkspaceDestination {
    pub workspace_id: String,
    #[schema(value_type = String)]
    pub path: PathBuf,
}

/// The answer to `POST /versions/uploads`.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionUploadResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    #[serde(flatten)]
    pub file: VersionUploadStart,
}

/// Where a file stands once its upload starts, in the response's `file` field.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
#[serde(tag = "file", rename_all = "snake_case")]
pub enum VersionUploadStart {
    /// Send parts 1 through `num_parts` to `upload_id`, each `part_size` bytes but the last.
    AwaitingParts {
        upload_id: String,
        part_size: u64,
        num_parts: u64,
        /// How many consecutive parts the upload accepts at once, starting at the first part not
        /// yet hashed. A part past them gets 429, so a client sending parts in order keeps at most
        /// this many in flight.
        part_window: u64,
    },
    /// The store already had the file, so it needs no parts, and it is staged at the destination,
    /// if one was given.
    AlreadyStored,
    /// A `file` value this build does not recognize. The server never sends it.
    #[serde(other, skip_serializing)]
    Unknown,
}

/// A part stored. `completed` is true for the part that completed the upload.
#[derive(Serialize, Deserialize, Debug, ToSchema)]
pub struct VersionUploadPartResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub completed: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CompleteVersionUploadRequest {
    pub files: Vec<CompletedFileUpload>,
    // If the workspace_id is provided, we will add the file to the workspace
    // otherwise, we will just add the file to the versions store
    pub workspace_id: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CleanCorruptedVersionsResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub result: CleanCorruptedVersionsResult,
}

#[derive(Deserialize, Serialize, Default, Debug, Clone)]
pub struct CleanCorruptedVersionsResult {
    // Number of version files scanned
    pub scanned: u64,
    // Number of version files that were corrupted(hash mismatch)
    pub corrupted: u64,
    // Number of version files that were successfully deleted
    pub cleaned: u64,
    // Errors that occurred during the cleanup process
    pub errors: u64,
    // Elapsed time in seconds
    pub elapsed: Duration,
}
