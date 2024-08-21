use actix_web::{web, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use futures_util::stream::StreamExt as _;
use liboxen::constants::NODES_DIR;
use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::constants::TREE_DIR;
use liboxen::view::StatusMessage;
use tar::Archive;

use liboxen::model::{MerkleHash, MerkleTreeNode};
use liboxen::repositories;
use liboxen::view::tree::nodes::{
    CommitNodeResponse, DirNodeResponse, FileNodeResponse, VNodeResponse,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

pub async fn get_node_by_id(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let node_id = path_param(&req, "node_id")?;

    let node_id = MerkleHash::from_str(&node_id)?;
    let node = repositories::tree::get_node_by_id(&repository, &node_id)?
        .ok_or(OxenHttpError::NotFound)?;

    node_to_json(node)
}

pub async fn create_node(
    req: HttpRequest,
    mut body: web::Payload,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;

    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }

    log::debug!(
        "create_node decompressing {} bytes",
        ByteSize::b(bytes.len() as u64)
    );

    let mut node_id: Option<MerkleHash> = None;
    let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
    match archive.entries() {
        Ok(entries) => {
            for file in entries {
                if let Ok(mut file) = file {
                    let path = file.path().unwrap();
                    let oxen_hidden_path = repository.path.join(OXEN_HIDDEN_DIR);
                    let dst_path = oxen_hidden_path.join(TREE_DIR).join(NODES_DIR).join(path);

                    if let Some(parent) = dst_path.parent() {
                        if !parent.exists() {
                            std::fs::create_dir_all(parent).expect("Could not create parent dir");
                        }
                    }
                    log::debug!("create_node writing {:?}", dst_path);
                    file.unpack(&dst_path).unwrap();

                    // the node_id is the last two path components combined
                    if !dst_path.ends_with("node") && !dst_path.ends_with("children") {
                        let id = dst_path
                            .components()
                            .rev()
                            .take(2)
                            .map(|c| c.as_os_str().to_str().unwrap())
                            .collect::<Vec<&str>>()
                            .into_iter()
                            .rev()
                            .collect::<String>();
                        node_id = Some(MerkleHash::from_str(&id)?);
                    }
                } else {
                    log::error!("Could not unpack file in archive...");
                }
            }
        }
        Err(err) => {
            log::error!("Could not unpack tree database from archive...");
            log::error!("Err: {:?}", err);
        }
    }

    if let Some(node_id) = node_id {
        let node = repositories::tree::get_node_by_id(&repository, &node_id)?
            .ok_or(OxenHttpError::NotFound)?;
        node_to_json(node)
    } else {
        log::error!("No node_id found in archive");
        Err(OxenHttpError::BadRequest(
            "No node_id found in archive".into(),
        ))
    }
}

fn node_to_json(node: MerkleTreeNode) -> actix_web::Result<HttpResponse, OxenHttpError> {
    match node {
        MerkleTreeNode::File(file) => Ok(HttpResponse::Ok().json(FileNodeResponse {
            status: StatusMessage::resource_found(),
            node: file,
        })),
        MerkleTreeNode::Directory(dir) => Ok(HttpResponse::Ok().json(DirNodeResponse {
            status: StatusMessage::resource_found(),
            node: dir,
        })),
        MerkleTreeNode::Commit(commit) => Ok(HttpResponse::Ok().json(CommitNodeResponse {
            status: StatusMessage::resource_found(),
            node: commit,
        })),
        MerkleTreeNode::VNode(vnode) => Ok(HttpResponse::Ok().json(VNodeResponse {
            status: StatusMessage::resource_found(),
            node: vnode,
        })),
        _ => Err(OxenHttpError::NotFound),
    }
}
