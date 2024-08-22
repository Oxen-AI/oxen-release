use flate2::write::GzEncoder;
use flate2::Compression;
use std::collections::HashSet;
use std::time;

use crate::api::client;
use crate::constants::{NODES_DIR, OXEN_HIDDEN_DIR, TREE_DIR};
use crate::core::v0_19_0::index::merkle_tree::node::merkle_node_db::node_db_path;
use crate::core::v0_19_0::index::merkle_tree::node::MerkleTreeNodeData;
use crate::error::OxenError;
use crate::model::{LocalRepository, MerkleHash, RemoteRepository};
use crate::view::{MerkleHashesResponse, StatusMessage};
use crate::{api, util};

pub async fn has_node(
    repository: &RemoteRepository,
    node_id: MerkleHash,
) -> Result<bool, OxenError> {
    let uri = format!("/tree/nodes/{node_id}");
    let url = api::endpoint::url_from_repo(repository, &uri)?;
    log::debug!("api::client::tree::get_by_id {}", url);

    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    if res.status() == 404 {
        return Ok(false);
    }

    let body = client::parse_json_body(&url, res).await?;
    log::debug!("api::client::tree::get_by_id Got response {}", body);
    let response: Result<StatusMessage, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(_) => Ok(true),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::get_by_id() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}

pub async fn create_node(
    local_repo: &LocalRepository,
    remote_repo: &RemoteRepository,
    node: &MerkleTreeNodeData,
) -> Result<(), OxenError> {
    // Compress the node
    let node_dir = node_db_path(local_repo, &node.hash);
    let tree_dir = local_repo
        .path
        .join(OXEN_HIDDEN_DIR)
        .join(TREE_DIR)
        .join(NODES_DIR);
    let sub_dir = util::fs::path_relative_to_dir(&node_dir, &tree_dir)?;
    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    log::debug!(
        "appending objects dir {:?} to tar at path {:?}",
        sub_dir,
        node_dir
    );
    tar.append_dir_all(sub_dir, node_dir)?;

    tar.finish()?;

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;

    // Upload the node
    let uri = format!("/tree/nodes");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::builder_for_url(&url)?
        .timeout(time::Duration::from_secs(120))
        .build()?;

    let size = buffer.len() as u64;
    println!(
        "uploading node of size {} to {}",
        bytesize::ByteSize::b(size),
        url
    );
    let res = client.post(&url).body(buffer.to_owned()).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    println!("upload node complete {}", body);

    Ok(())
}

pub async fn list_missing_file_hashes(
    remote_repo: &RemoteRepository,
    node_id: &MerkleHash,
) -> Result<HashSet<MerkleHash>, OxenError> {
    let uri = format!("/tree/nodes/{node_id}/missing_file_hashes");
    let url = api::endpoint::url_from_repo(remote_repo, &uri)?;
    let client = client::new_for_url(&url)?;
    let res = client.get(&url).send().await?;
    let body = client::parse_json_body(&url, res).await?;
    let response: Result<MerkleHashesResponse, serde_json::Error> = serde_json::from_str(&body);
    match response {
        Ok(response) => Ok(response.hashes),
        Err(err) => Err(OxenError::basic_str(format!(
            "api::client::tree::list_missing_file_hashes() Could not deserialize response [{err}]\n{body}"
        ))),
    }
}
