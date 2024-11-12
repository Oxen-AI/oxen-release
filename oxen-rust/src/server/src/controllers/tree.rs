use actix_web::{web, HttpRequest, HttpResponse};
use bytesize::ByteSize;
use flate2::read::GzDecoder;
use futures_util::stream::StreamExt as _;
use liboxen::constants::NODES_DIR;
use liboxen::constants::OXEN_HIDDEN_DIR;
use liboxen::constants::TREE_DIR;
use liboxen::core::v0_19_0::index::merkle_node_db::node_db_path;
use liboxen::core::v0_19_0::index::merkle_node_db::node_db_prefix;
use liboxen::error::OxenError;
use liboxen::model::Commit;
use liboxen::model::LocalRepository;
use liboxen::view::tree::merkle_hashes::MerkleHashes;
use liboxen::view::MerkleHashesResponse;
use liboxen::view::StatusMessage;

use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::HashSet;
use std::path::Path;
use tar::Archive;

use std::path::PathBuf;
use std::str::FromStr;

use liboxen::model::merkle_tree::node::{EMerkleTreeNode, MerkleTreeNode};
use liboxen::model::MerkleHash;
use liboxen::repositories;
use liboxen::view::tree::nodes::{
    CommitNodeResponse, DirNodeResponse, FileNodeResponse, VNodeResponse,
};

use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::TreeDepthQuery;
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

pub async fn list_missing_node_hashes(
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

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    log::debug!(
        "list_missing_node_hashes checking {} node ids",
        request.hashes.len()
    );
    let hashes = repositories::tree::list_missing_node_hashes(&repository, &request.hashes)?;
    log::debug!(
        "list_missing_node_hashes found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn list_missing_file_hashes_from_commits(
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

    let request: MerkleHashes = serde_json::from_slice(&bytes)?;
    log::debug!(
        "list_missing_file_hashes_from_commits checking {} commit ids",
        request.hashes.len()
    );
    let hashes =
        repositories::tree::list_missing_file_hashes_from_commits(&repository, &request.hashes)?;
    log::debug!(
        "list_missing_file_hashes_from_commits found {} missing node ids",
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
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
    log::debug!(
        "list_missing_file_hashes {} got {} hashes",
        hash,
        hashes.len()
    );
    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn create_nodes(
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

    let mut hashes: HashSet<MerkleHash> = HashSet::new();
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
            hashes.insert(MerkleHash::from_str(&id)?);
        }
    }

    Ok(HttpResponse::Ok().json(MerkleHashesResponse {
        status: StatusMessage::resource_found(),
        hashes,
    }))
}

pub async fn download_tree(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    // Download the entire tree
    let buffer = compress_tree(&repository)?;

    Ok(HttpResponse::Ok().body(buffer))
}

pub async fn download_tree_nodes(
    req: HttpRequest,
    query: web::Query<TreeDepthQuery>,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;
    let base_head_str = path_param(&req, "base_head")?;

    log::debug!("download_tree_nodes for base_head: {}", base_head_str);
    log::debug!(
        "download_tree_nodes subtree: {:?}, depth: {:?}",
        query.subtree,
        query.depth
    );

    let (base_commit_id, maybe_head_commit_id) = maybe_parse_base_head(base_head_str)?;

    let base_commit = repositories::commits::get_by_id(&repository, &base_commit_id)?
        .ok_or(OxenError::resource_not_found(&base_commit_id))?;

    // Could be a single commit or a range of commits
    let commits = get_commit_list(
        &repository,
        &base_commit,
        maybe_head_commit_id,
        &query.subtree,
    )?;

    // zip up the node directories for each commit tree
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Collect the unique node hashes for all the commits
    let unique_node_hashes =
        get_unique_node_hashes(&repository, &commits, &query.subtree, &query.depth)?;

    for hash in unique_node_hashes {
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = node_db_prefix(&hash);
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        let node_dir = node_db_path(&repository, &hash);
        log::debug!("Compressing node from dir {:?}", node_dir);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed {} commits size is {}",
        commits.len(),
        ByteSize::b(total_size)
    );

    Ok(HttpResponse::Ok().body(buffer))
}

fn get_commit_list(
    repository: &LocalRepository,
    base_commit: &Commit,
    maybe_head_commit_id: Option<String>,
    maybe_subtree: &Option<PathBuf>,
) -> Result<Vec<Commit>, OxenError> {
    // If we have a head commit, then we are downloading a range of commits
    // Otherwise, we are downloading all commits from the base commit back to the first commit
    // This is the difference between the first pull and subsequent pulls
    // The first pull doesn't have a head commit, but subsequent pulls do
    let commits = if let Some(head_commit_id) = maybe_head_commit_id {
        let head_commit = repositories::commits::get_by_id(repository, &head_commit_id)?
            .ok_or(OxenError::resource_not_found(&head_commit_id))?;
        repositories::commits::list_between(repository, &head_commit, base_commit)?
    } else {
        // If the subtree is specified, we only want to get the latest commit
        if maybe_subtree.is_some() {
            vec![base_commit.clone()]
        } else {
            repositories::commits::list_from(repository, &base_commit.id)?
        }
    };

    Ok(commits)
}

fn get_unique_node_hashes(
    repository: &LocalRepository,
    commits: &[Commit],
    maybe_subtree: &Option<PathBuf>,
    maybe_depth: &Option<i32>,
) -> Result<HashSet<MerkleHash>, OxenError> {
    // Collect the unique node hashes for all the commits
    // There could be duplicate nodes across commits, hence the need to dedup
    let mut unique_node_hashes: HashSet<MerkleHash> = HashSet::new();
    for commit in commits {
        let tree = repositories::tree::get_subtree_by_depth(
            repository,
            commit,
            maybe_subtree,
            maybe_depth,
        )?;

        tree.walk_tree_without_leaves(|node| {
            unique_node_hashes.insert(node.hash);
        });

        // Add the commit hash itself
        unique_node_hashes.insert(commit.hash()?);
    }

    Ok(unique_node_hashes)
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

pub async fn download_commits(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let base_head = path_param(&req, "base_head")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let (base_commit_id, maybe_head_commit_id) = maybe_parse_base_head(base_head)?;

    let base_commit = repositories::commits::get_by_id(&repository, &base_commit_id)?
        .ok_or(OxenError::resource_not_found(&base_commit_id))?;

    // If we have a head commit, then we are downloading a range of commits
    // Otherwise, we are downloading all commits from the base commit back to the first commit
    // This is the difference between the first pull and subsequent pulls
    // The first pull doesn't have a head commit, but subsequent pulls do
    let commits = if let Some(head_commit_id) = maybe_head_commit_id {
        let head_commit = repositories::commits::get_by_id(&repository, &head_commit_id)?
            .ok_or(OxenError::resource_not_found(&head_commit_id))?;
        repositories::commits::list_between(&repository, &head_commit, &base_commit)?
    } else {
        repositories::commits::list_from(&repository, &base_commit_id)?
    };

    // zip up the node directory
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    for commit in &commits {
        let hash = commit.hash()?;
        // This will be the subdir within the tarball
        // so when we untar it, all the subdirs will be extracted to
        // tree/nodes/...
        let dir_prefix = node_db_prefix(&hash);
        let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR).join(dir_prefix);

        let node_dir = node_db_path(&repository, &hash);
        log::debug!("Compressing commit from dir {:?}", node_dir);
        if node_dir.exists() {
            tar.append_dir_all(&tar_subdir, node_dir)?;
        }
    }
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    log::debug!(
        "Compressed {} commits size is {}",
        commits.len(),
        ByteSize::b(total_size)
    );

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

fn compress_tree(repository: &LocalRepository) -> Result<Vec<u8>, OxenError> {
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);
    compress_full_tree(repository, &mut tar)?;
    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);

    log::debug!("Compressed entire tree size is {}", ByteSize::b(total_size));

    Ok(buffer)
}

fn compress_full_tree(
    repository: &LocalRepository,
    tar: &mut tar::Builder<GzEncoder<Vec<u8>>>,
) -> Result<(), OxenError> {
    // This will be the subdir within the tarball,
    // so when we untar it, all the subdirs will be extracted to
    // tree/nodes/...
    let tar_subdir = Path::new(TREE_DIR).join(NODES_DIR);
    let nodes_dir = repository
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);

    log::debug!("Compressing tree in dir {:?}", nodes_dir);

    if nodes_dir.exists() {
        tar.append_dir_all(&tar_subdir, nodes_dir)?;
    }

    Ok(())
}

fn node_to_json(node: MerkleTreeNode) -> actix_web::Result<HttpResponse, OxenHttpError> {
    match node.node {
        EMerkleTreeNode::File(file) => Ok(HttpResponse::Ok().json(FileNodeResponse {
            status: StatusMessage::resource_found(),
            node: file,
        })),
        EMerkleTreeNode::Directory(dir) => Ok(HttpResponse::Ok().json(DirNodeResponse {
            status: StatusMessage::resource_found(),
            node: dir,
        })),
        EMerkleTreeNode::Commit(commit) => Ok(HttpResponse::Ok().json(CommitNodeResponse {
            status: StatusMessage::resource_found(),
            node: commit,
        })),
        EMerkleTreeNode::VNode(vnode) => Ok(HttpResponse::Ok().json(VNodeResponse {
            status: StatusMessage::resource_found(),
            node: vnode,
        })),
        _ => Err(OxenHttpError::NotFound),
    }
}

/// Parses a base..head string into a base and head string
/// If the base..head string does not contain a .., then it returns the base as the base and head as None
fn maybe_parse_base_head(
    base_head: impl AsRef<str>,
) -> Result<(String, Option<String>), OxenError> {
    let base_head_str = base_head.as_ref();
    if base_head_str.contains("..") {
        let mut split = base_head_str.split("..");
        if let (Some(base), Some(head)) = (split.next(), split.next()) {
            Ok((base.to_string(), Some(head.to_string())))
        } else {
            Err(OxenError::basic_str(
                "Could not parse commits. Format should be base..head",
            ))
        }
    } else {
        Ok((base_head_str.to_string(), None))
    }
}
