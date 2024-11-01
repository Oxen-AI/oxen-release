use std::path::Path;

use rmp_serde::Serializer;
use serde::Serialize;

use crate::core::v0_19_0;
use crate::core::v0_19_0::structs::StagedMerkleTreeNode;
use crate::error::OxenError;
use crate::model::merkle_tree::node::FileNode;
use crate::model::merkle_tree::node::MerkleTreeNode;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataTabular;
use crate::model::Schema;
use crate::model::StagedEntryStatus;
use crate::model::Workspace;
use crate::repositories;

/// Updates the staged schema by changing the column name from `before_column` to `after_column`
/// and updating the metadata from the original schema.
pub fn update_schema(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    og_schema: &Schema,
    before_column: &str,
    after_column: &str,
) -> Result<(), OxenError> {
    let mut schema = og_schema.clone();
    if let Some(og_field) = og_schema.fields.iter().find(|f| f.name == before_column) {
        for field in &mut schema.fields {
            if field.name == before_column {
                field.name = after_column.to_string();
                field.metadata = og_field.metadata.clone();
                break;
            }
        }
    }

    let db = v0_19_0::data_frames::schemas::get_staged_db(&workspace.workspace_repo)?;
    let key = path.as_ref().to_string_lossy();

    let data = db.get(key.as_bytes())?;
    let mut file_node: FileNode;

    if let Some(data) = data {
        let val: Result<StagedMerkleTreeNode, rmp_serde::decode::Error> =
            rmp_serde::from_slice(data.as_slice());
        file_node = val.unwrap().node.file()?;
    } else {
        file_node = repositories::tree::get_file_by_path(
            &workspace.base_repo,
            &workspace.commit,
            path.as_ref(),
        )?
        .ok_or(OxenError::basic_str("File not found"))?;
    }

    if let Some(GenericMetadata::MetadataTabular(tabular_metadata)) = &file_node.metadata {
        file_node.metadata = Some(GenericMetadata::MetadataTabular(MetadataTabular::new(
            tabular_metadata.tabular.width,
            tabular_metadata.tabular.height,
            schema,
        )));
    } else {
        return Err(OxenError::basic_str("Expected tabular metadata"));
    }

    let staged_entry_node = MerkleTreeNode::from_file(file_node.clone());
    let staged_entry = StagedMerkleTreeNode {
        status: StagedEntryStatus::Modified,
        node: staged_entry_node.clone(),
    };

    let mut buf = Vec::new();
    staged_entry
        .serialize(&mut Serializer::new(&mut buf))
        .unwrap();
    db.put(key.as_bytes(), &buf)?;

    Ok(())
}
