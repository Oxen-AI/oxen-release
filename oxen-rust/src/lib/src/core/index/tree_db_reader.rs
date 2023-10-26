use crate::core::db::tree_db::TreeDB;
use crate::error::OxenError;
use crate::model::repository::local_repository::LocalRepository;
use rocksdb::SingleThreaded;
use std::path::PathBuf;
pub struct TreeDBReader {
    pub client_db: TreeDB<SingleThreaded>,
    pub server_db: TreeDB<SingleThreaded>,
    pub lca_db: TreeDB<SingleThreaded>, // TODONOW: multithread if needed
}


// TODONOW naming of these (client, server etc)
// TODONOW: do we actually need these repos here or in treedb land...
impl TreeDBReader {
    pub fn new(
        repo: &LocalRepository,
        client_db_path: PathBuf,
        server_db_path: PathBuf,
        lca_db_path: PathBuf,
    ) -> Result<TreeDBReader, OxenError> {
        let client_db = TreeDB::new_read_only(repo, &client_db_path)?;
        let server_db = TreeDB::new_read_only(repo, &server_db_path)?;
        let lca_db = TreeDB::new_read_only(repo, &lca_db_path)?;
        Ok(TreeDBReader {
            client_db,
            server_db,
            lca_db,
        })
    }
}