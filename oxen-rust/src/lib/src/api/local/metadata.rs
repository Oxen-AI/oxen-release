//! Helper functions to get metadata from the local filesystem.
//!

use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::MetaData;
use crate::model::MetaDataEntry;
use crate::util;

use std::path::Path;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
pub fn compute_metadata(path: impl AsRef<Path>) -> Result<MetaDataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    // MetaData based on data_type
    let meta = get_file_metadata(path, &data_type)?;

    Ok(MetaDataEntry {
        filename: base_name.to_string_lossy().to_string(),
        is_dir: path.is_dir(),
        latest_commit: None,
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        meta,
    })
}

/// Returns the file size in bytes.
pub fn get_file_size(path: impl AsRef<Path>) -> Result<u64, OxenError> {
    let metadata = std::fs::metadata(path.as_ref())?;
    Ok(metadata.len())
}

/// Returns metadata based on data_type
pub fn get_file_metadata(
    path: impl AsRef<Path>,
    data_type: &EntryDataType,
) -> Result<MetaData, OxenError> {
    match data_type {
        EntryDataType::Text => Ok(MetaData {
            text: Some(text::get_metadata(path)?),
            image: None,
            video: None,
            audio: None,
            tabular: None,
        }),
        EntryDataType::Image => Ok(MetaData {
            text: None,
            image: Some(image::get_metadata(path)?),
            video: None,
            audio: None,
            tabular: None,
        }),
        EntryDataType::Video => Ok(MetaData {
            text: None,
            image: None,
            video: Some(video::get_metadata(path)?),
            audio: None,
            tabular: None,
        }),
        EntryDataType::Audio => Ok(MetaData {
            text: None,
            image: None,
            video: None,
            audio: Some(audio::get_metadata(path)?),
            tabular: None,
        }),
        EntryDataType::Tabular => Ok(MetaData {
            text: None,
            image: None,
            video: None,
            audio: None,
            tabular: Some(tabular::get_metadata(path)?),
        }),
        _ => Ok(MetaData {
            image: None,
            text: None,
            video: None,
            audio: None,
            tabular: None,
        }),
    }
}
