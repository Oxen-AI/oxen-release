//! Helper functions to get metadata from the local filesystem.
//!

use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::MetadataItem;
use crate::model::MetadataEntry;
use crate::util;

use std::path::Path;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
pub fn compute_metadata(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    // MetaData based on data_type
    let meta = get_file_metadata(path, &data_type)?;

    Ok(MetadataEntry {
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
) -> Result<MetadataItem, OxenError> {
    match data_type {
        EntryDataType::Text => Ok(MetadataItem {
            text: Some(text::get_metadata(path)?),
            image: None,
            video: None,
            audio: None,
            tabular: None,
        }),
        EntryDataType::Image => Ok(MetadataItem {
            text: None,
            image: Some(image::get_metadata(path)?),
            video: None,
            audio: None,
            tabular: None,
        }),
        EntryDataType::Video => Ok(MetadataItem {
            text: None,
            image: None,
            video: Some(video::get_metadata(path)?),
            audio: None,
            tabular: None,
        }),
        EntryDataType::Audio => Ok(MetadataItem {
            text: None,
            image: None,
            video: None,
            audio: Some(audio::get_metadata(path)?),
            tabular: None,
        }),
        EntryDataType::Tabular => Ok(MetadataItem {
            text: None,
            image: None,
            video: None,
            audio: None,
            tabular: Some(tabular::get_metadata(path)?),
        }),
        _ => Ok(MetadataItem {
            image: None,
            text: None,
            video: None,
            audio: None,
            tabular: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::EntryDataType;
    use crate::test;

    #[test]
    fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = api::local::metadata::compute_metadata(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");
    }
}
