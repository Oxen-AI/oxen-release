//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::index::commit_entry_reader::CommitEntryReader;
use crate::core::index::CommitReader;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::MetadataDir;
use crate::model::{Commit, CommitEntry, LocalRepository, MetadataEntry};
use crate::util;

use std::path::Path;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
pub fn get(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    let metadata = get_file_metadata(path, &data_type)?;

    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        is_dir: path.is_dir(),
        latest_commit: None,
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
    })
}

/// Returns the metadata given a file path
pub fn from_path(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path.file_name().ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);
    let metadata = get_file_metadata(path, &data_type)?;

    // TODO: how do we get the cached dir info if the entry is a dir?
    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        is_dir: path.is_dir(),
        latest_commit: None,
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
    })
}

pub fn from_commit_entry(
    repo: &LocalRepository,
    entry: &CommitEntry,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    let path = util::fs::version_path(repo, entry);
    let base_name = entry
        .path
        .file_name()
        .ok_or(OxenError::file_has_no_name(&path))?;
    let size = get_file_size(&path)?;
    let mime_type = util::fs::file_mime_type(&path);
    let data_type = util::fs::datatype_from_mimetype(&path, mime_type.as_str());
    let extension = util::fs::file_extension(&path);
    let metadata = get_file_metadata(&path, &data_type)?;

    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        is_dir: path.is_dir(),
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
    })
}

/// Returns metadata with latest commit information. Less efficient than get().
pub fn get_cli(
    repo: &LocalRepository,
    entry_path: impl AsRef<Path>,
    data_path: impl AsRef<Path>,
) -> Result<CLIMetadataEntry, OxenError> {
    let path = data_path.as_ref();
    let entry_path = entry_path.as_ref();
    let base_name = entry_path
        .file_name()
        .ok_or(OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let hash = util::hasher::hash_file_contents(path)?;
    let mime_type = util::fs::file_mime_type(path);
    let data_type = util::fs::datatype_from_mimetype(path, mime_type.as_str());
    let extension = util::fs::file_extension(path);

    let commit_reader = CommitReader::new(repo)?;

    // Not the most efficient, if there are a ton of commits, but it's the easiest way to get the last updated commit
    let mut last_updated: Option<Commit> = None;
    // Sort commits by timestamp
    let mut commits = commit_reader.list_all()?;
    commits.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    // Now that we know the commits are sorted, we can iterate through them and find when the file was last updated
    for commit in commits {
        log::debug!("looking for entry in commit {commit}");
        let commit_entry_reader = CommitEntryReader::new(repo, &commit)?;
        match commit_entry_reader.get_entry(entry_path) {
            Ok(Some(entry)) => {
                log::debug!(
                    "considering commit {} for file {} and entry.hash {} current hash {}",
                    commit,
                    entry_path.display(),
                    entry.hash,
                    hash
                );
                if last_updated.is_none() {
                    last_updated = Some(commit.clone());
                }

                let latest = last_updated.as_ref().unwrap();

                // make sure the commit is newer than the last one
                // and that the hash is the same as the current version
                // if the hash is the same as the current data, this is the latest commit given that file
                if commit.timestamp >= latest.timestamp && entry.hash == hash {
                    last_updated = Some(commit);
                    break;
                }
            }
            Ok(None) => {
                continue;
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    Ok(CLIMetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        last_updated,
        hash,
        size,
        data_type,
        mime_type,
        extension,
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
) -> Result<Option<GenericMetadata>, OxenError> {
    match data_type {
        EntryDataType::Dir => Ok(Some(GenericMetadata::MetadataDir(MetadataDir {
            data_types: vec![],
        }))),
        EntryDataType::Text => Ok(Some(GenericMetadata::MetadataText(text::get_metadata(
            path,
        )?))),
        EntryDataType::Image => Ok(Some(GenericMetadata::MetadataImage(image::get_metadata(
            path,
        )?))),
        EntryDataType::Video => Ok(Some(GenericMetadata::MetadataVideo(video::get_metadata(
            path,
        )?))),
        EntryDataType::Audio => Ok(Some(GenericMetadata::MetadataAudio(audio::get_metadata(
            path,
        )?))),
        EntryDataType::Tabular => Ok(Some(GenericMetadata::MetadataTabular(
            tabular::get_metadata(path)?,
        ))),
        _ => Ok(None),
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
        let metadata = api::local::metadata::get(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");
    }
}
