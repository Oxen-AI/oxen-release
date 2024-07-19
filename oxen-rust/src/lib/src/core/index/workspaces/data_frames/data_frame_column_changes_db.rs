use rocksdb::DB;
use std::path::PathBuf;

use crate::{
    core::{db, index::merger},
    error::OxenError,
    model::{Commit, LocalRepository},
    util,
    view::data_frames::DataFrameColumnChange,
};

pub fn write_data_frame_column_change(
    data_frame_column_change: &DataFrameColumnChange,
    db_dir_path: &PathBuf,
) -> Result<(), OxenError> {
    let opts = db::key_val::opts::default();
    let db = DB::open(&opts, dunce::simplified(db_dir_path))?;

    save_data_frame_column_changes(&db, data_frame_column_change)
}

pub fn save_data_frame_column_changes(
    db: &DB,
    data_frame_column_change: &DataFrameColumnChange,
) -> Result<(), OxenError> {
    let key = &data_frame_column_change.column_name;
    let val_json = serde_json::to_string(data_frame_column_change)?;

    db.put(key, val_json.as_bytes())?;

    log::debug!(
        "save_data_frame_column_changes() saved change: {:?}",
        data_frame_column_change
    );

    Ok(())
}

// pub fn get_conflict_commit(&self) -> Result<Option<Commit>, OxenError> {
//     let hidden_dir = util::fs::oxen_hidden_dir(&self.repository.path);
//     let merge_head_path = hidden_dir.join(MERGE_HEAD_FILE);
//     let commit_id = util::fs::read_first_line(merge_head_path)?;
//     let commit_reader = CommitReader::new(&self.repository)?;
//     commit_reader.get_commit_by_id(commit_id)
// }
