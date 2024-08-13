pub mod commit_node;
pub mod deserialized_node;
pub mod dir_node;
pub mod file_chunk_node;
pub mod file_node;
pub mod file_node_types;
pub mod merkle_tree_node_data;
pub mod merkle_tree_node_type;
pub mod schema_node;
pub mod vnode;

pub use commit_node::CommitNode;
pub use deserialized_node::DeserializedMerkleTreeNode;
pub use dir_node::DirNode;
pub use file_chunk_node::FileChunkNode;
pub use file_node::FileNode;
pub use file_node_types::{FileChunkType, FileStorageType};
pub use merkle_tree_node_data::MerkleTreeNodeData;
pub use merkle_tree_node_type::MerkleTreeNodeType;
pub use schema_node::SchemaNode;
pub use vnode::VNode;

pub trait MerkleTreeNode {
    fn dtype(&self) -> MerkleTreeNodeType;
    fn id(&self) -> u128;
}
