use actix_web::{web, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use futures_util::stream::StreamExt as _;
use liboxen::constants::NODES_DIR;
use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::constants::TREE_DIR;
use liboxen::core::v0_19_0::index::merkle_tree::node::merkle_node_db::node_db_path;
use liboxen::core::v0_19_0::index::merkle_tree::node::merkle_node_db::node_db_prefix;
use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::StatusMessage;

use flate2::write::GzEncoder;
use flate2::Compression;
use std::path::Path;
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
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;

    let node =
        repositories::tree::get_node_by_id(&repository, &hash)?.ok_or(OxenHttpError::NotFound)?;

    node_to_json(node)
}

pub async fn list_missing_file_hashes(
    req: HttpRequest,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, repo_name)?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;

    let hashes = repositories::tree::list_missing_file_hashes(&repository, &hash)?;
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
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

    let mut hash: Option<MerkleHash> = None;
    let mut archive = Archive::new(GzDecoder::new(&bytes[..]));
    let Ok(entries) = archive.entries() else {
        return Err(OxenHttpError::BadRequest(
            "Could not unpack tree database from archive".into(),
        ));
    };

    for file in entries {
        let Ok(mut file) = file else {
            log::error!("Could not unpack file in archive...");
            continue;
        };
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

        // the hash is the last two path components combined
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
            hash = Some(MerkleHash::from_str(&id)?);
        }
    }

    if let Some(hash) = hash {
        let node = repositories::tree::get_node_by_id(&repository, &hash)?
            .ok_or(OxenHttpError::NotFound)?;
        node_to_json(node)
    } else {
        log::error!("No hash found in archive");
        Err(OxenHttpError::BadRequest("No hash found in archive".into()))
    }
}

pub async fn download_tree(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;

    let buffer = compress_tree(&repository, &hash)?;

    Ok(HttpResponse::Ok().body(buffer))
}

pub async fn download_node(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let hash_str = path_param(&req, "hash")?;
    let hash = MerkleHash::from_str(&hash_str)?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let buffer = compress_node(&repository, &hash)?;

    Ok(HttpResponse::Ok().body(buffer))
}

fn compress_node(repository: &LocalRepository, hash: &MerkleHash) -> Result<Vec<u8>, OxenError> {
    // This will be the subdir within the tarball
    // so when we untar it, all the subdirs will be extracted to
    // tree/nodes/...
    let dir_prefix = node_db_prefix(hash);
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

    // zip up the node directory
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    let node_dir = node_db_path(repository, hash);

    log::debug!("Compressing node {} from dir {:?}", hash, node_dir);
    if node_dir.exists() {
        tar.append_dir_all(&tar_subdir, node_dir)?;
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed node {} size is {}",
        hash,
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

fn compress_tree(repository: &LocalRepository, hash: &MerkleHash) -> Result<Vec<u8>, OxenError> {
    log::debug!("Compressing entire tree {}", hash);
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    r_compress_tree(repository, hash, &mut tar)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed tree {} size is {}",
        hash,
        ByteSize::b(total_size)
    );

    Ok(buffer)
}

fn r_compress_tree(
    repository: &LocalRepository,
    hash: &MerkleHash,
    tar: &mut tar::Builder<GzEncoder<Vec<u8>>>,
) -> Result<(), OxenError> {
    // This will be the subdir within the tarball,
    // so when we untar it, all the subdirs will be extracted to
    // tree/nodes/...
    let dir_prefix = node_db_prefix(hash);
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

    let node_dir = node_db_path(repository, hash);
    log::debug!("Compressing tree node {} from dir {:?}", hash, node_dir);

    if node_dir.exists() {
        log::debug!("Tree node {} exists, adding to tarball", hash);
        tar.append_dir_all(&tar_subdir, node_dir)?;

        let Some(node) = repositories::tree::get_node_by_id(&repository, &hash)? else {
            return Err(OxenError::basic_str(format!("Node {} not found", hash)));
        };

        let has_children = node.has_children();
        log::debug!("Got node {} has children {}", node, has_children);
        if has_children {
            let children = repositories::tree::child_hashes(&repository, &hash)?;
            for child in children {
                r_compress_tree(repository, &child, tar)?;
            }
        }
    }

    Ok(())
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
