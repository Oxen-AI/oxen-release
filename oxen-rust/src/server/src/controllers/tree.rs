use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use liboxen::core::v0_10_0::cache::commit_cacher;

use liboxen::error::OxenError;
use liboxen::model::NewCommitBody;
use liboxen::view::workspaces::{ListWorkspaceResponseView, NewWorkspace, WorkspaceResponse};
use liboxen::view::{CommitResponse, StatusMessage, WorkspaceResponseView};
use liboxen::{core::v0_10_0::index, repositories};

use actix_web::{HttpRequest, HttpResponse};

pub mod changes;
pub mod data_frames;
pub mod files;

pub async fn get_node_children(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    // Take in the node id, and return which children are missing

    return Ok(HttpResponse::BadRequest().json(StatusMessage::error("Implement me!".to_string())));
}