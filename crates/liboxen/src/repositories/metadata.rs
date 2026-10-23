//! Helper functions to get metadata from the local filesystem.
//!

use crate::core::df::tabular::s3_cloud_options;
use crate::error::OxenError;
use crate::model::entry::entry_data_type::EntryDataType;
use crate::model::entry::metadata_entry::CLIMetadataEntry;
use crate::model::merkle_tree::node::{DirNode, FileNode};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::parsed_resource::ParsedResourceView;
use crate::model::{Commit, LocalRepository, MerkleHash, MetadataEntry};
use crate::opts::CountLinesOpts;
use crate::repositories;
use crate::storage::VersionStore;
use crate::util;
use crate::util::fs::{
    LineCounts, data_type_from_head, file_extension, mime_type_from_head, read_head,
};

use bytes::Bytes;
use polars::io::cloud::CloudOptions;
use polars::io::mmap::MmapBytesReader;
use std::fmt;
use std::fs::File;
use std::io::{self, Cursor, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;

pub mod audio;
pub mod image;
pub mod tabular;
pub mod text;
pub mod video;

/// Returns the metadata given a file path
#[cfg(test)]
fn get(path: impl AsRef<Path>) -> Result<MetadataEntry, OxenError> {
    let path = path.as_ref();
    let base_name = path
        .file_name()
        .ok_or_else(|| OxenError::file_has_no_name(path))?;
    let size = get_file_size(path)?;
    let (mime_type, data_type, metadata) = inspect_file(path, path)?;
    let extension = util::fs::file_extension(path);

    Ok(MetadataEntry {
        filename: base_name.to_string_lossy().to_string(),
        hash: "".to_string(),
        is_dir: path.is_dir(),
        latest_commit: None,
        resource: None,
        size,
        data_type,
        mime_type,
        extension,
        metadata,
        is_queryable: None,
        children: None,
    })
}

pub fn from_file_node(
    _repo: &LocalRepository,
    node: &FileNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name().to_string(),
        hash: node.hash().to_string(),
        is_dir: false,
        latest_commit: Some(commit.to_owned()),
        resource: Some(ParsedResourceView {
            commit: Some(commit.to_owned()),
            branch: None,
            workspace: None,
            path: PathBuf::from(node.name()),
            version: PathBuf::from(commit.id.to_string()),
            resource: PathBuf::from(commit.id.to_string()).join(node.name()),
        }),
        size: node.num_bytes(),
        data_type: node.data_type().clone(),
        mime_type: node.mime_type().to_string(),
        extension: node.extension().to_string(),
        metadata: node.metadata(),
        is_queryable: None,
        children: None,
    })
}

pub fn from_dir_node(
    _repo: &LocalRepository,
    node: &DirNode,
    commit: &Commit,
) -> Result<MetadataEntry, OxenError> {
    Ok(MetadataEntry {
        filename: node.name().to_string(),
        hash: node.hash().to_string(),
        is_dir: true,
        latest_commit: Some(commit.to_owned()),
        resource: None,
        size: node.num_bytes(),
        data_type: EntryDataType::Dir,
        mime_type: "inode/directory".to_string(),
        extension: "".to_string(),
        metadata: None,
        is_queryable: None,
        children: None,
    })
}

/// Returns metadata with latest commit information.
pub use crate::core::v_latest::metadata::get_cli;

/// Returns CLI metadata for `path` as it existed at `revision`, sourced from the merkle tree
/// rather than the working-tree file. `revision` may be a branch name, a commit id, or `HEAD`.
/// Errors if `revision` does not resolve to a commit, or if `path` is not a file in that commit.
pub fn get_cli_at_revision(
    repo: &LocalRepository,
    path: impl AsRef<Path>,
    revision: &str,
) -> Result<CLIMetadataEntry, OxenError> {
    let path = path.as_ref();
    let commit = repositories::revisions::get(repo, revision)?
        .ok_or_else(|| OxenError::RevisionNotFound(revision.into()))?;
    let file_node = repositories::tree::get_file_by_path(repo, &commit, path)?
        .ok_or_else(|| OxenError::entry_does_not_exist_in_commit(path, &commit.id))?;
    let last_commit_id = file_node.last_commit_id().to_string();
    let last_updated = repositories::commits::get_by_id(repo, &last_commit_id)?;
    Ok(CLIMetadataEntry {
        filename: file_node.name().to_string(),
        last_updated,
        hash: file_node.hash().to_string(),
        size: file_node.num_bytes(),
        data_type: file_node.data_type().clone(),
        mime_type: file_node.mime_type().to_string(),
        extension: file_node.extension().to_string(),
    })
}

/// Returns the file size in bytes.
pub fn get_file_size(path: impl AsRef<Path>) -> Result<u64, OxenError> {
    let metadata = util::fs::metadata(path.as_ref())?;
    Ok(metadata.len())
}

/// A file's bytes, in a file at a path, in memory, or in an S3 version store.
pub(crate) enum ByteSource<'a> {
    Path(&'a Path),
    Memory(Bytes),
    Stored(Box<StoredVersion>),
}

impl ByteSource<'_> {
    /// A reader of the bytes. A stored version file's reader blocks while it fetches them, so read
    /// it off the async runtime.
    pub(crate) fn open(&self) -> Result<Box<dyn MmapBytesReader>, OxenError> {
        match self {
            ByteSource::Path(path) => Ok(Box::new(
                File::open(path).map_err(|err| OxenError::file_open_error(path, err))?,
            )),
            ByteSource::Memory(data) => Ok(Box::new(Cursor::new(data.clone()))),
            ByteSource::Stored(version) => Ok(Box::new(version.reader())),
        }
    }

    /// What [`util::fs::count_lines`] gives for `data`, a reader of these bytes, which is left
    /// unread when the lines were counted as the bytes arrived.
    pub(crate) fn count_lines(
        &self,
        data: impl Read,
        opts: CountLinesOpts,
    ) -> Result<(usize, Option<usize>), OxenError> {
        if let ByteSource::Stored(version) = self
            && let Some(lines) = version.lines
        {
            let (line_count, chars) = lines.lines(opts.remove_trailing_blank_line);
            if chars.is_some() || !opts.with_chars {
                return Ok((line_count, chars.filter(|_| opts.with_chars)));
            }
        }
        util::fs::count_lines(data, opts)
    }
}

impl fmt::Debug for ByteSource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ByteSource::Path(path) => write!(f, "{path:?}"),
            ByteSource::Memory(data) => write!(f, "{} bytes in memory", data.len()),
            ByteSource::Stored(version) => {
                write!(f, "version file {} at {}", version.hash, version.url)
            }
        }
    }
}

/// A version file in an S3 version store, with its line counts when they were taken as it arrived.
pub(crate) struct StoredVersion {
    store: Arc<dyn VersionStore>,
    hash: String,
    size: u64,
    pub(crate) url: String,
    pub(crate) cloud_options: CloudOptions,
    lines: Option<LineCounts>,
    runtime: Handle,
}

impl StoredVersion {
    /// The version file `hash`, which `store` keeps at `url` in `region`, behind `endpoint_url`
    /// when the store is not AWS. Call on the async runtime.
    pub(crate) async fn new(
        store: Arc<dyn VersionStore>,
        hash: MerkleHash,
        url: String,
        region: &str,
        endpoint_url: Option<&str>,
        lines: Option<LineCounts>,
    ) -> Result<Self, OxenError> {
        let hash = hash.to_string();
        let size = store.get_version_size(&hash).await?;
        let cloud_options = s3_cloud_options(&url, region, endpoint_url)?;
        Ok(Self {
            store,
            hash,
            size,
            url,
            cloud_options,
            lines,
            runtime: Handle::current(),
        })
    }

    pub(crate) fn size(&self) -> u64 {
        self.size
    }

    /// A seekable reader of the file, which fetches the ranges it reads from the store.
    pub(crate) fn reader(&self) -> VersionReader {
        VersionReader {
            store: Arc::clone(&self.store),
            hash: self.hash.clone(),
            size: self.size,
            runtime: self.runtime.clone(),
            position: 0,
            fetched: Bytes::new(),
            fetched_at: 0,
        }
    }
}

/// The fewest bytes a [`VersionReader`] fetches at once, 64 KiB.
const MIN_FETCH: u64 = 64 * 1024;
/// The most bytes a [`VersionReader`] fetches at once to read ahead of a run of reads, 8 MiB.
const MAX_FETCH: u64 = 8 * 1024 * 1024;

/// A blocking, seekable reader of a version file in the store, which fetches the ranges it reads.
pub(crate) struct VersionReader {
    store: Arc<dyn VersionStore>,
    hash: String,
    size: u64,
    runtime: Handle,
    position: u64,
    fetched: Bytes,
    fetched_at: u64,
}

impl Read for VersionReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.position >= self.size {
            return Ok(0);
        }
        let fetched_to = self.fetched_at + self.fetched.len() as u64;
        if self.position < self.fetched_at || self.position >= fetched_to {
            let ahead = if self.position == fetched_to && !self.fetched.is_empty() {
                (self.fetched.len() as u64 * 2).min(MAX_FETCH)
            } else {
                MIN_FETCH
            };
            let len = ahead.max(buf.len() as u64).min(self.size - self.position);
            let fetch = self.store.get_version_chunk(&self.hash, self.position, len);
            let chunk = self.runtime.block_on(fetch).map_err(io::Error::other)?;
            if chunk.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "the version store returned no bytes before the file's end",
                ));
            }
            self.fetched = Bytes::from(chunk);
            self.fetched_at = self.position;
        }
        let start = (self.position - self.fetched_at) as usize;
        let n = buf.len().min(self.fetched.len() - start);
        buf[..n].copy_from_slice(&self.fetched[start..start + n]);
        self.position += n as u64;
        Ok(n)
    }
}

impl Seek for VersionReader {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let position = match pos {
            SeekFrom::Start(offset) => Some(offset),
            SeekFrom::End(offset) => self.size.checked_add_signed(offset),
            SeekFrom::Current(offset) => self.position.checked_add_signed(offset),
        };
        self.position = position.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek before the start of the file",
            )
        })?;
        Ok(self.position)
    }
}

impl MmapBytesReader for VersionReader {}

/// Reads the MIME type, data type, and metadata of the file at `data_path`, whose name is
/// `file_path`. Fails when the file cannot be opened or read, and gives no metadata for content
/// that cannot be parsed.
pub(crate) fn inspect_file(
    data_path: &Path,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&ByteSource::Path(data_path), file_path)
}

/// Reads the MIME type, data type, and metadata of `data`, the bytes of a file named `file_path`.
/// Gives no metadata for content that cannot be parsed.
pub(crate) fn inspect_bytes(
    data: Bytes,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&ByteSource::Memory(data), file_path)
}

/// Reads the MIME type, data type, and metadata of `version`, the bytes of a file named
/// `file_path`, fetching what it reads from the store. Blocks while it reads them. Gives no
/// metadata for content that cannot be parsed.
pub(crate) fn inspect_stored(
    version: StoredVersion,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    inspect(&ByteSource::Stored(Box::new(version)), file_path)
}

fn inspect(
    source: &ByteSource,
    file_path: &Path,
) -> Result<(String, EntryDataType, Option<GenericMetadata>), OxenError> {
    let mut reader = source.open()?;
    let head = read_head(&mut reader).map_err(|err| match source {
        ByteSource::Path(data_path) => OxenError::file_read_error(data_path, err),
        ByteSource::Memory(_) | ByteSource::Stored(_) => err.into(),
    })?;
    let mime_type = mime_type_from_head(&head, file_path);
    let data_type = data_type_from_head(&head, file_path, &mime_type);
    let metadata = get_file_metadata(&mut reader, source, file_path, &data_type)?;
    Ok((mime_type, data_type, metadata))
}

/// Returns the metadata of `data`, the bytes of a file of `data_type` named `file_path`. A tabular
/// file's metadata, and line counts taken as a stored file arrived, come from `source` instead,
/// which holds the same bytes as `data`.
fn get_file_metadata(
    data: &mut (impl Read + Seek),
    source: &ByteSource,
    file_path: &Path,
    data_type: &EntryDataType,
) -> Result<Option<GenericMetadata>, OxenError> {
    let extension = file_extension(file_path);
    data.rewind()?;
    match data_type {
        EntryDataType::Text => match text::get_metadata(data, source) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataText(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute text metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Image => match image::get_metadata(data) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataImage(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute image metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Video => match video::get_metadata(data) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataVideo(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute video metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Audio => match audio::get_metadata(data, &extension) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataAudio(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute audio metadata"
                );
                Ok(None)
            }
        },
        EntryDataType::Tabular => match tabular::get_metadata_with_extension(source, &extension) {
            Ok(metadata) => Ok(Some(GenericMetadata::MetadataTabular(metadata))),
            Err(err) => {
                tracing::error!(
                    oxen.file_path = %file_path.display(),
                    exception.message = %err,
                    "Could not compute tabular metadata"
                );
                Ok(None)
            }
        },
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::{inspect_bytes, inspect_file};
    use crate::core::df::tabular;
    use crate::error::OxenError;
    use crate::model::EntryDataType;
    use crate::repositories;
    use crate::test;
    use crate::util;
    use crate::util::hasher::maybe_get_metadata_hash;
    use bytes::Bytes;
    use polars::df;
    use std::fs;
    use std::path::Path;

    #[tokio::test]
    async fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");
    }

    #[test]
    fn test_inspect_bytes_matches_inspect_file() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let arrow = dir.join("frame.arrow");
            tabular::write_df(
                &mut df!("id" => [1, 2, 3], "label" => ["a", "b", "c"])?,
                &arrow,
            )?;
            let files = [
                test::test_csv_file_with_name("mixed_data_types.csv"),
                test::test_spam_ham(),
                test::test_text_jsonl(),
                test::test_text_json(),
                test::test_1k_parquet(),
                arrow,
                test::test_img_file_with_name("cat_1.jpg"),
                test::test_audio_file_with_name("121-121726-0005.wav"),
                test::test_video_file_with_name("basketball.mp4"),
                test::test_text_file_with_name("hello.txt"),
            ];
            for file in files {
                let (mime_type, data_type, metadata) = inspect_file(&file, &file)?;
                assert!(metadata.is_some(), "{file:?} has metadata to compare");
                let (bytes_mime_type, bytes_data_type, bytes_metadata) =
                    inspect_bytes(Bytes::from(fs::read(&file)?), &file)?;
                assert_eq!(
                    (bytes_mime_type, bytes_data_type),
                    (mime_type, data_type),
                    "{file:?}"
                );
                assert_eq!(
                    maybe_get_metadata_hash(&bytes_metadata)?,
                    maybe_get_metadata_hash(&metadata)?,
                    "{file:?} gets the same metadata from memory as from the file"
                );
            }
            Ok(())
        })
    }

    #[tokio::test]
    async fn test_get_cli_at_revision_reads_committed_metadata_not_worktree()
    -> Result<(), OxenError> {
        test::run_empty_local_repo_test_async(|repo| async move {
            let relative_path = Path::new("hello.txt");
            let full_path = repo.path.join(relative_path);

            util::fs::write_to_path(&full_path, "head version")?;
            repositories::add(&repo, &full_path).await?;
            repositories::commit(&repo, "add hello")?;

            let committed =
                repositories::metadata::get_cli_at_revision(&repo, relative_path, "HEAD")?;

            // Change the working tree to prove the metadata comes from the commit, not disk.
            util::fs::write_to_path(&full_path, "a longer working-tree version with new bytes")?;

            let at_head =
                repositories::metadata::get_cli_at_revision(&repo, relative_path, "HEAD")?;
            assert_eq!(at_head.hash, committed.hash);
            assert_eq!(at_head.size, "head version".len() as u64);

            Ok(())
        })
        .await
    }
}
