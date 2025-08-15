//! Wrapper around std::fs commands to make them easier to use
//! and eventually abstract away the fs implementation
//!

use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use jwalk::WalkDir;

use simdutf8::compat::from_utf8;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::io::SeekFrom;

use crate::constants;
use crate::constants::CACHE_DIR;
use crate::constants::CHUNKS_DIR;
use crate::constants::CONTENT_IS_VALID;
use crate::constants::DATA_ARROW_FILE;
use crate::constants::HISTORY_DIR;
use crate::constants::OXEN_HIDDEN_DIR;
use crate::constants::TREE_DIR;
use crate::constants::VERSION_FILE_NAME;
use crate::core::versions::MinOxenVersion;
use crate::error::OxenError;
use crate::model::entry::commit_entry::Entry;
use crate::model::merkle_tree::node::FileNode;
use crate::model::metadata::metadata_image::ImgResize;
use crate::model::Commit;
use crate::model::Schema;
use crate::model::{CommitEntry, EntryDataType, LocalRepository};
use crate::opts::CountLinesOpts;
use crate::storage::version_store::VersionStore;
use crate::view::health::DiskUsage;
use filetime::FileTime;
use image::{ImageFormat, ImageReader};

use crate::repositories;
use crate::util;

// Deprecated
pub fn oxen_hidden_dir(repo_path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(repo_path.as_ref()).join(Path::new(constants::OXEN_HIDDEN_DIR))
}

pub fn oxen_tmp_dir() -> Result<PathBuf, OxenError> {
    // Override the cache dir with the OXEN_TMP_DIR env var if it is set
    if let Ok(tmp_dir) = std::env::var("OXEN_TMP_DIR") {
        return Ok(PathBuf::from(tmp_dir));
    }

    match dirs::cache_dir() {
        Some(cache_dir) => Ok(cache_dir.join(constants::OXEN)),
        None => Err(OxenError::cache_dir_not_found()),
    }
}

pub fn oxen_config_dir() -> Result<PathBuf, OxenError> {
    // Override the home dir with the OXEN_CONFIG_DIR env var if it is set
    if let Ok(config_dir) = std::env::var("OXEN_CONFIG_DIR") {
        return Ok(PathBuf::from(config_dir));
    }

    match dirs::home_dir() {
        Some(home_dir) => Ok(home_dir.join(constants::CONFIG_DIR).join(constants::OXEN)),
        None => Err(OxenError::home_dir_not_found()),
    }
}

pub fn config_filepath(repo_path: &Path) -> PathBuf {
    oxen_hidden_dir(repo_path).join(constants::REPO_CONFIG_FILENAME)
}

pub fn repo_exists(repo_path: &Path) -> bool {
    oxen_hidden_dir(repo_path).exists()
}

pub fn commit_content_is_valid_path(repo: &LocalRepository, commit: &Commit) -> PathBuf {
    oxen_hidden_dir(&repo.path)
        .join(HISTORY_DIR)
        .join(&commit.id)
        .join(CACHE_DIR)
        .join(CONTENT_IS_VALID)
}

pub fn version_path_for_commit_id(
    repo: &LocalRepository,
    commit_id: &str,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    match repositories::commits::get_by_id(repo, commit_id)? {
        Some(commit) => match repositories::entries::get_commit_entry(repo, &commit, filepath)? {
            Some(entry) => {
                let path = version_path(repo, &entry);
                let arrow_path = path.parent().unwrap().join(DATA_ARROW_FILE);
                if arrow_path.exists() {
                    Ok(arrow_path)
                } else {
                    Ok(path)
                }
            }
            None => Err(OxenError::path_does_not_exist(filepath)),
        },
        None => Err(OxenError::revision_not_found(commit_id.into())),
    }
}

pub fn handle_image_resize(
    version_store: Arc<dyn VersionStore>,
    file_hash: String,
    file_path: &Path,
    version_path: &Path,
    img_resize: ImgResize,
) -> Result<PathBuf, OxenError> {
    log::debug!("img_resize {:?}", img_resize);
    let resized_path = resized_path_for_version_store_file(
        Arc::clone(&version_store),
        &file_hash,
        file_path,
        img_resize.width,
        img_resize.height,
    )?;

    resize_cache_image_version_store(
        version_store,
        &file_hash,
        version_path,
        &resized_path,
        img_resize,
    )?;

    log::debug!("In the resize cache! {:?}", resized_path);
    Ok(resized_path)
}

// TODO: Change the resized path from version store to a server location
pub fn resized_path_for_version_store_file(
    version_store: Arc<dyn VersionStore>,
    img_hash: &str,
    img_path: &Path,
    width: Option<u32>,
    height: Option<u32>,
) -> Result<PathBuf, OxenError> {
    let img_version_path = version_store.get_version_path(img_hash)?;
    let extension = img_path.extension().unwrap().to_str().unwrap();
    let width = width.map(|w| w.to_string());
    let height = height.map(|w| w.to_string());
    let resized_path = img_version_path.parent().unwrap().join(format!(
        "{}x{}.{}",
        width.unwrap_or("".to_string()),
        height.unwrap_or("".to_string()),
        extension
    ));
    Ok(resized_path)
}

pub fn version_file_size(repo: &LocalRepository, entry: &CommitEntry) -> Result<u64, OxenError> {
    let version_path = version_path(repo, entry);
    // if is_tabular(&version_path) {
    //     let data_file = version_path.parent().unwrap().join(DATA_ARROW_FILE);
    //     if !data_file.exists() {
    //         // just for unit tests to pass for now, we only really call file size from the server
    //         // on the client we don't have the data.arrow file and would have to compute size on the fly
    //         // but a warning for now should be good
    //         log::warn!("TODO: compute size of data file: {:?}", data_file);
    //         return Ok(0);
    //     }
    //     let meta = util::fs::metadata(&data_file)?;
    //     Ok(meta.len())
    // } else {
    if !version_path.exists() {
        return Err(OxenError::path_does_not_exist(version_path));
    }
    let meta = util::fs::metadata(&version_path)?;
    Ok(meta.len())
    // }
}

pub fn chunk_path(repo: &LocalRepository, hash: impl AsRef<str>) -> PathBuf {
    oxen_hidden_dir(&repo.path)
        .join(TREE_DIR)
        .join(CHUNKS_DIR)
        .join(hash.as_ref())
        .join("data")
}

pub fn version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => version_path_from_hash_and_file_v0_10_0(
            &repo.path,
            entry.hash.clone(),
            entry.filename(),
        ),
        _ => version_path_from_hash_and_file(&repo.path, entry.hash.clone(), entry.filename()),
    }
}

pub fn root_version_path(path: impl AsRef<Path>) -> PathBuf {
    util::fs::oxen_hidden_dir(path).join(constants::VERSIONS_DIR)
}

pub fn version_path_from_hash_and_filename(
    repo: &LocalRepository,
    hash: impl AsRef<str>,
    filename: impl AsRef<Path>,
) -> PathBuf {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            version_path_from_hash_and_file_v0_10_0(&repo.path, hash, filename)
        }
        _ => version_path_from_hash_and_file(&repo.path, hash, filename),
    }
}

pub fn version_path_from_node(
    repo: &LocalRepository,
    file_hash: impl AsRef<str>,
    path: impl AsRef<Path>,
) -> PathBuf {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            version_path_from_hash_and_file_v0_10_0(&repo.path, file_hash, path)
        }
        _ => version_path_from_hash_and_file(&repo.path, file_hash, path),
    }
}

pub fn version_path_from_hash(repo: &LocalRepository, hash: impl AsRef<str>) -> PathBuf {
    match repo.min_version() {
        MinOxenVersion::V0_10_0 => {
            version_path_from_hash_and_file_v0_10_0(&repo.path, hash, PathBuf::new())
        }
        _ => version_path_from_hash_and_file(&repo.path, hash, PathBuf::new()),
    }
}

pub fn version_path_for_entry(repo: &LocalRepository, entry: &Entry) -> PathBuf {
    match entry {
        Entry::CommitEntry(commit_entry) => version_path(repo, commit_entry),
        Entry::SchemaEntry(schema_entry) => {
            version_path_from_schema_hash(repo.path.clone(), schema_entry.hash.clone())
        }
    }
}

pub fn version_path_from_dst(dst: impl AsRef<Path>, entry: &CommitEntry) -> PathBuf {
    version_path_from_hash_and_file(dst, entry.hash.clone(), entry.filename())
}

pub fn version_path_from_dst_generic(dst: impl AsRef<Path>, entry: &Entry) -> PathBuf {
    match entry {
        Entry::CommitEntry(commit_entry) => version_path_from_dst(dst, commit_entry),
        Entry::SchemaEntry(schema_entry) => {
            version_path_from_schema_hash(dst, schema_entry.hash.clone())
        }
    }
}

pub fn df_version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    let version_dir = version_dir_from_hash(&repo.path, entry.hash.clone());
    version_dir.join(DATA_ARROW_FILE)
}

pub fn version_path_from_hash_and_file_v0_10_0(
    dst: impl AsRef<Path>,
    hash: impl AsRef<str>,
    filename: impl AsRef<Path>,
) -> PathBuf {
    let hash = hash.as_ref();
    let filename = filename.as_ref();
    let version_dir = version_dir_from_hash(dst, hash);
    // log::debug!(
    //     "version_path_from_hash_and_file version_dir {:?}",
    //     version_dir
    // );
    let extension = extension_from_path(filename);
    version_dir.join(format!("{}.{}", VERSION_FILE_NAME, extension))
}

pub fn version_path_from_hash_and_file(
    dst: impl AsRef<Path>,
    hash: impl AsRef<str>,
    filename: impl AsRef<Path>,
) -> PathBuf {
    let hash = hash.as_ref();
    let filename = filename.as_ref();
    let version_dir = version_dir_from_hash(dst, hash);
    // log::debug!(
    //     "version_path_from_hash_and_file version_dir {:?}",
    //     version_dir
    // );
    let extension = extension_from_path(filename);
    if extension.is_empty() {
        version_dir.join(VERSION_FILE_NAME)
    } else {
        // backwards compatibility
        let path = version_dir.join(format!("{}.{}", VERSION_FILE_NAME, extension));
        if path.exists() {
            // Older files have the extension in the filename
            path
        } else {
            // Newer files do not have the extension in the filename
            version_dir.join(VERSION_FILE_NAME)
        }
    }
}

pub fn version_path_from_schema(dst: impl AsRef<Path>, schema: &Schema) -> PathBuf {
    // Save schemas as path with no extension
    version_path_from_schema_hash(dst, schema.hash.clone())
}

pub fn version_path_from_schema_hash(dst: impl AsRef<Path>, hash: String) -> PathBuf {
    // Save schemas as path with no extension
    let version_dir = version_dir_from_hash(dst, hash);
    version_dir.join(VERSION_FILE_NAME)
}

pub fn extension_from_path(path: &Path) -> String {
    if let Some(ext) = path.extension() {
        String::from(ext.to_str().unwrap_or(""))
    } else {
        String::from("")
    }
}

pub fn version_dir_from_hash(dst: impl AsRef<Path>, hash: impl AsRef<str>) -> PathBuf {
    let hash = hash.as_ref();
    let topdir = &hash[..2];
    let subdir = &hash[2..];
    oxen_hidden_dir(dst.as_ref())
        .join(constants::VERSIONS_DIR)
        .join(constants::FILES_DIR)
        .join(topdir)
        .join(subdir)
}

pub fn object_dir_suffix_from_hash(_dst: impl AsRef<Path>, hash: String) -> PathBuf {
    let topdir = &hash[..2];
    let subdir = &hash[2..];

    PathBuf::from(topdir).join(subdir)
}

pub fn read_bytes_from_path(path: impl AsRef<Path>) -> Result<Vec<u8>, OxenError> {
    let path = path.as_ref();
    Ok(std::fs::read(path)?)
}

pub fn read_file(path: Option<impl AsRef<Path>>) -> Result<String, OxenError> {
    match path {
        Some(path) => read_from_path(path),
        None => Ok(String::new()),
    }
}

pub fn read_from_path(path: impl AsRef<Path>) -> Result<String, OxenError> {
    let path = path.as_ref();
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(_) => {
            let err = format!(
                "util::fs::read_from_path could not open: {}",
                path.display()
            );
            log::warn!("{}", err);
            Err(OxenError::basic_str(&err))
        }
    }
}

pub fn write_to_path(path: impl AsRef<Path>, value: impl AsRef<str>) -> Result<(), OxenError> {
    let path = path.as_ref();
    let value = value.as_ref();

    // Make sure the parent directory exists
    if let Some(parent) = path.parent() {
        create_dir_all(parent)?;
    }

    match File::create(path) {
        Ok(mut file) => match file.write(value.as_bytes()) {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "Could not write file {path:?}\n{err}"
            ))),
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not create file to write {path:?}\n{err}"
        ))),
    }
}

pub fn write_data(path: &Path, data: &[u8]) -> Result<(), OxenError> {
    match File::create(path) {
        Ok(mut file) => match file.write(data) {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "Could not write file {path:?}\n{err}"
            ))),
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not create file to write {path:?}\n{err}"
        ))),
    }
}

pub fn append_to_file(path: &Path, value: &str) -> Result<(), OxenError> {
    match OpenOptions::new().append(true).open(path) {
        Ok(mut file) => match file.write(value.as_bytes()) {
            Ok(_) => Ok(()),
            Err(err) => Err(OxenError::basic_str(format!(
                "Could not append to file {path:?}\n{err}"
            ))),
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not open file to append {path:?}\n{err}"
        ))),
    }
}

pub fn count_lines(
    path: impl AsRef<Path>,
    opts: CountLinesOpts,
) -> Result<(usize, Option<usize>), OxenError> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let mut reader = BufReader::with_capacity(1024 * 32, file);
    let mut line_count = 1;
    let mut char_count = 0;
    let mut last_buf: Vec<u8> = Vec::new();
    let mut char_option: Option<usize> = None;

    loop {
        let len = {
            let buf = reader.fill_buf()?;

            if buf.is_empty() {
                break;
            }

            if opts.remove_trailing_blank_line {
                last_buf = buf.to_vec();
            }

            if opts.with_chars {
                char_count += bytecount::num_chars(buf);
            }

            line_count += bytecount::count(buf, b'\n');
            buf.len()
        };
        reader.consume(len);
    }

    if let Some(last_byte) = last_buf.last() {
        if last_byte == &b'\n' {
            line_count -= 1;
        }
    }

    if opts.with_chars {
        char_option = Some(char_count);
    }

    Ok((line_count, char_option))
}

pub fn read_lines_file(file: &File) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let reader = BufReader::new(file);
    // read all the lines of the file into a Vec<String>
    for line in reader.lines().map_while(Result::ok) {
        lines.push(line);
    }
    lines
}

pub fn read_first_n_bytes(path: impl AsRef<Path>, n: usize) -> Result<Vec<u8>, OxenError> {
    let mut file = File::open(path.as_ref())?;
    let mut buffer = vec![0; n];
    let bytes_read = file.read(&mut buffer)?;
    buffer.truncate(bytes_read);
    Ok(buffer)
}

pub fn read_first_line(path: impl AsRef<Path>) -> Result<String, OxenError> {
    let file = File::open(path.as_ref())?;
    read_first_line_from_file(&file)
}

pub fn read_first_line_from_file(file: &File) -> Result<String, OxenError> {
    let reader = BufReader::new(file);
    if let Some(Ok(line)) = reader.lines().next() {
        Ok(line)
    } else {
        Err(OxenError::basic_str(format!(
            "Could not read line from file: {file:?}"
        )))
    }
}

pub fn read_first_byte_from_file(path: impl AsRef<Path>) -> Result<char, OxenError> {
    let mut file = File::open(path)?;
    let mut buffer = [0; 1]; // Single byte buffer
    file.read_exact(&mut buffer)?;
    let first_char = buffer[0] as char;
    Ok(first_char)
}

pub fn read_lines(path: &Path) -> Result<Vec<String>, OxenError> {
    let file = File::open(path)?;
    Ok(read_lines_file(&file))
}

pub fn read_lines_paginated(path: &Path, start: usize, size: usize) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut i = 0;
            let mut line = String::from("");
            while let Ok(len) = reader.read_line(&mut line) {
                if i >= (start + size) || 0 == len {
                    break;
                }

                if i >= start {
                    lines.push(line.trim().to_string());
                }
                line.clear();
                i += 1;
            }
        }
        Err(_) => {
            eprintln!("Could not open staging file {}", path.display())
        }
    }
    lines
}

pub fn read_lines_paginated_ret_size(
    path: &Path,
    start: usize,
    size: usize,
) -> (Vec<String>, usize) {
    let mut i = 0;
    let mut lines: Vec<String> = Vec::new();
    match File::open(path) {
        Ok(file) => {
            let mut reader = BufReader::new(file);
            let mut line = String::from("");
            while let Ok(len) = reader.read_line(&mut line) {
                if 0 == len {
                    break;
                }

                if i >= start && i < (start + size) {
                    lines.push(line.trim().to_string());
                }
                line.clear();
                i += 1;
            }
        }
        Err(_) => {
            eprintln!("Could not open staging file {}", path.display())
        }
    }
    (lines, i)
}

pub fn list_dirs_in_dir(dir: &Path) -> Result<Vec<PathBuf>, OxenError> {
    let mut dirs: Vec<PathBuf> = vec![];
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            dirs.push(path);
        }
    }
    Ok(dirs)
}

pub fn list_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(paths) => {
            for path in paths.flatten() {
                if path.path().is_file() {
                    files.push(path.path());
                }
            }
        }
        Err(err) => {
            eprintln!(
                "util::fs::list_files_in_dir Could not find dir: {} err: {}",
                dir.display(),
                err
            )
        }
    }

    files
}

pub fn rlist_paths_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                files.push(path);
            }
            Err(err) => eprintln!("rlist_paths_in_dir Could not iterate over dir... {err}"),
        }
    }
    files
}

/// Recursively lists directories in a repo that are not .oxen directories
pub fn rlist_dirs_in_repo(repo: &LocalRepository) -> Vec<PathBuf> {
    let dir = &repo.path;
    let mut dirs: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return dirs;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if path.is_dir() && !is_in_oxen_hidden_dir(&path) {
                    dirs.push(path);
                }
            }
            Err(err) => log::error!("rlist_dirs_in_repo Could not iterate over dir... {err}"),
        }
    }
    dirs
}

/// Recursively tries to traverse up for an .oxen directory, returns None if not found
pub fn get_repo_root(path: &Path) -> Option<PathBuf> {
    if path.join(OXEN_HIDDEN_DIR).exists() {
        return Some(path.to_path_buf());
    }

    if let Some(parent) = path.parent() {
        get_repo_root(parent)
    } else {
        None
    }
}

pub fn get_repo_root_from_current_dir() -> Option<PathBuf> {
    let Ok(path) = std::env::current_dir() else {
        log::error!("Could not get current directory");
        return None;
    };
    get_repo_root(&path)
}

pub fn copy_dir_all(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Result<(), OxenError> {
    // There is not a recursive copy in the standard library, so we implement it here
    let from = from.as_ref();
    let to = to.as_ref();
    log::debug!(
        "copy_dir_all Copy directory from: {:?} -> to: {:?}",
        from,
        to
    );

    let mut stack = Vec::new();
    stack.push(PathBuf::from(from));

    let output_root = PathBuf::from(to);
    let input_root = PathBuf::from(from).components().count();

    while let Some(working_path) = stack.pop() {
        // log::debug!("copy_dir_all process: {:?}", &working_path);

        // Generate a relative path
        let src: PathBuf = working_path.components().skip(input_root).collect();

        // Create a destination if missing
        let dest = if src.components().count() == 0 {
            output_root.clone()
        } else {
            output_root.join(&src)
        };
        if !dest.exists() {
            // log::debug!("copy_dir_all  mkdir: {:?}", dest);
            util::fs::create_dir_all(&dest)?;
        }

        for entry in std::fs::read_dir(working_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                match path.file_name() {
                    Some(filename) => {
                        let dest_path = dest.join(filename);
                        // log::debug!("copy_dir_all   copy: {:?} -> {:?}", &path, &dest_path);
                        std::fs::copy(&path, &dest_path)?;
                    }
                    None => {
                        log::error!("copy_dir_all could not get file_name: {:?}", path);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Wrapper around the std::fs::copy command to tell us which file failed to copy
pub fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    let max_retries = 3;
    let mut attempt = 0;

    while attempt < max_retries {
        match std::fs::copy(src, dst) {
            Ok(_) => return Ok(()),
            Err(err) => {
                if err.raw_os_error() == Some(32) {
                    attempt += 1;
                    if attempt == max_retries {
                        return Err(OxenError::basic_str(format!(
                            "File is in use by another process after {} attempts: {src:?}",
                            max_retries
                        )));
                    }
                    // Exponential backoff: 100ms, 200ms, 400ms
                    let sleep_duration = std::time::Duration::from_millis(100 * 2_u64.pow(attempt));
                    std::thread::sleep(sleep_duration);
                    continue;
                } else if !src.exists() {
                    return Err(OxenError::file_error(src, err));
                } else {
                    return Err(OxenError::file_copy_error(src, dst, err));
                }
            }
        }
    }

    // This should never be reached due to the return statements above
    unreachable!()
}

/// Wrapper around the std::fs::rename command to tell us which file failed to copy
pub fn rename(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    let dst = dst.as_ref();

    // Platform-specific behavior
    // This function currently corresponds to the rename function on Unix and the MoveFileEx function with the MOVEFILE_REPLACE_EXISTING flag on Windows.
    if cfg!(windows) {
        // If we are moving, make sure to make the parent
        if let Some(parent) = dst.parent() {
            create_dir_all(parent)?;
        }

        // copy then delete on windows :shrug:
        if src.is_file() {
            copy(src, dst)?;
            remove_file(src)
        } else {
            copy_dir_all(src, dst)?;
            remove_dir_all(src)
        }
    } else {
        match std::fs::rename(src, dst) {
            Ok(_) => Ok(()),
            Err(err) => {
                if !src.exists() {
                    Err(OxenError::file_error(src, err))
                } else {
                    Err(OxenError::file_rename_error(src, dst, err))
                }
            }
        }
    }
}

/// Wrapper around the std::fs::copy which makes the parent directory of the dst if it doesn't exist
pub fn copy_mkdir(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    let dst = dst.as_ref();
    if let Some(parent) = dst.parent() {
        create_dir_all(parent)?;
    }
    match std::fs::copy(src, dst) {
        Ok(_) => Ok(()),
        Err(err) => {
            if !src.exists() {
                Err(OxenError::file_error(src, err))
            } else {
                Err(OxenError::file_copy_error(src, dst, err))
            }
        }
    }
}

/// Recursively check if a file exists within a directory
pub fn file_exists_in_directory(directory: impl AsRef<Path>, file: impl AsRef<Path>) -> bool {
    let mut file = file.as_ref();
    while file.parent().is_some() {
        if directory.as_ref() == file.parent().unwrap() {
            return true;
        }
        file = file.parent().unwrap();
    }
    false
}

/// Wrapper around the util::fs::create_dir_all command to tell us which file it failed on
/// creates a directory if they don't exist
pub fn create_dir_all(src: impl AsRef<Path>) -> Result<(), OxenError> {
    if src.as_ref().exists() {
        return Ok(());
    }

    let src = src.as_ref();
    match std::fs::create_dir_all(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("create_dir_all {:?} {}", src, err);
            Err(OxenError::file_error(src, err))
        }
    }
}

/// Wrapper around the util::fs::remove_dir_all command to tell us which file it failed on
pub fn remove_dir_all(src: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    match std::fs::remove_dir_all(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("remove_dir_all {:?} {}", src, err);
            Err(OxenError::file_error(src, err))
        }
    }
}

/// Wrapper around the std::fs::write command to tell us which file it failed on
pub fn write(src: impl AsRef<Path>, data: impl AsRef<[u8]>) -> Result<(), OxenError> {
    let src = src.as_ref();
    match std::fs::write(src, data) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("write {:?} {}", src, err);
            Err(OxenError::file_error(src, err))
        }
    }
}

/// Wrapper around the util::fs::remove_file command to tell us which file it failed on
pub fn remove_file(src: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    log::debug!("Removing file: {}", src.display());
    match std::fs::remove_file(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("remove_file {:?} {}", src, err);
            Err(OxenError::file_error(src, err))
        }
    }
}

/// Wrapper around util::fs::metadata to give us a better error on failure
pub fn metadata(path: impl AsRef<Path>) -> Result<std::fs::Metadata, OxenError> {
    let path = path.as_ref();
    match std::fs::metadata(path) {
        Ok(file) => Ok(file),
        Err(err) => {
            log::debug!("metadata {:?} {}", path, err);
            Err(OxenError::file_metadata_error(path, err))
        }
    }
}

/// Wrapper around std::fs::File::create to give us a better error on failure
pub fn file_create(path: impl AsRef<Path>) -> Result<std::fs::File, OxenError> {
    let path = path.as_ref();
    match std::fs::File::create(path) {
        Ok(file) => Ok(file),
        Err(err) => {
            log::error!("file_create {:?} {}", path, err);
            Err(OxenError::file_create_error(path, err))
        }
    }
}

/// Looks at both the extension and the first bytes of the file to determine if it is tabular
pub fn is_tabular_from_extension(data_path: impl AsRef<Path>, file_path: impl AsRef<Path>) -> bool {
    let data_path = data_path.as_ref();
    let file_path = file_path.as_ref();
    if has_ext(file_path, "json") {
        // check if the first character in the file is '['
        // if so it is just a json array we can treat as tabular
        if let Ok(c) = read_first_byte_from_file(data_path) {
            if "[" == c.to_string() {
                return true;
            }
        }
    }

    has_tabular_extension(file_path)
}

/// Looks at the extension of the file to determine if it is tabular
pub fn has_tabular_extension(file_path: impl AsRef<Path>) -> bool {
    let file_path = file_path.as_ref();
    let exts: HashSet<String> = vec!["csv", "tsv", "parquet", "arrow", "ndjson", "jsonl"]
        .into_iter()
        .map(String::from)
        .collect();
    contains_ext(file_path, &exts)
}

pub fn is_tabular(path: &Path) -> bool {
    is_tabular_from_extension(path, path)
}

pub fn is_tabular_with_ext(path: &Path, data_path: &Path) -> bool {
    is_tabular_from_extension(data_path, path)
}

pub fn is_image(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["jpg", "png"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_markdown(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["md"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_video(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["mp4"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_audio(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["mp3", "wav"].into_iter().map(String::from).collect();
    contains_ext(path, &exts)
}

pub fn is_utf8(path: &Path) -> bool {
    const SAMPLE_SIZE: usize = 4096;

    let bytes = match read_first_n_bytes(path, SAMPLE_SIZE) {
        Ok(b) => b,
        Err(_) => return false,
    };

    if bytes.is_empty() {
        return true;
    }

    match from_utf8(&bytes) {
        Ok(_) => true,
        Err(e) => e.error_len().is_none(),
    }
}

pub fn data_type_from_extension(path: &Path) -> EntryDataType {
    let ext = path.extension().unwrap_or_default().to_string_lossy();
    match ext.as_ref() {
        "json" => EntryDataType::Tabular,
        "csv" => EntryDataType::Tabular,
        "tsv" => EntryDataType::Tabular,
        "parquet" => EntryDataType::Tabular,
        "arrow" => EntryDataType::Tabular,
        "ndjson" => EntryDataType::Tabular,
        "jsonl" => EntryDataType::Tabular,

        "md" => EntryDataType::Text,
        "txt" => EntryDataType::Text,
        "html" => EntryDataType::Text,
        "xml" => EntryDataType::Text,
        "yaml" => EntryDataType::Text,
        "yml" => EntryDataType::Text,
        "toml" => EntryDataType::Text,

        "png" => EntryDataType::Image,
        "jpg" => EntryDataType::Image,
        "jpeg" => EntryDataType::Image,
        "gif" => EntryDataType::Image,
        "bmp" => EntryDataType::Image,
        "tiff" => EntryDataType::Image,
        "heic" => EntryDataType::Image,
        "heif" => EntryDataType::Image,
        "webp" => EntryDataType::Image,

        "mp4" => EntryDataType::Video,
        "avi" => EntryDataType::Video,
        "mov" => EntryDataType::Video,

        "mp3" => EntryDataType::Audio,
        "wav" => EntryDataType::Audio,
        "aac" => EntryDataType::Audio,
        "ogg" => EntryDataType::Audio,
        "flac" => EntryDataType::Audio,
        "opus" => EntryDataType::Audio,

        _ => EntryDataType::Binary,
    }
}

pub fn file_mime_type(path: &Path) -> String {
    file_mime_type_from_extension(path, path)
}

// We have this split out because we get the mime type from the extension
// but the data type from the contents
// and the version path does not always have the extension in newer versions of oxen
pub fn file_mime_type_from_extension(data_path: &Path, file_path: &Path) -> String {
    match infer::get_from_path(data_path) {
        Ok(Some(kind)) => {
            // log::debug!("file_mime_type {:?} {}", data_path, kind.mime_type());
            String::from(kind.mime_type())
        }
        _ => {
            if is_markdown(file_path) {
                String::from("text/markdown")
            } else if is_utf8(data_path) {
                String::from("text/plain")
            } else if data_path.is_dir() {
                String::from("inode/directory")
            } else {
                // https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types/Common_types
                // application/octet-stream is the default value for all other cases.
                // An unknown file type should use this type.
                // Browsers are particularly careful when manipulating these files to
                // protect users from software vulnerabilities and possible dangerous behavior.
                String::from("application/octet-stream")
            }
        }
    }
}

pub fn datatype_from_mimetype(data_path: &Path, mime_type: &str) -> EntryDataType {
    datatype_from_mimetype_from_extension(data_path, data_path, mime_type)
}

// We have this split out because we get the mime type from the extension
// but the data type from the contents
// and the version path does not always have the extension in newer versions of oxen
pub fn datatype_from_mimetype_from_extension(
    data_path: &Path,
    file_path: &Path,
    mime_type: &str,
) -> EntryDataType {
    match mime_type {
        // Image
        "image/jpeg" => EntryDataType::Image,
        "image/png" => EntryDataType::Image,
        "image/gif" => EntryDataType::Image,
        "image/webp" => EntryDataType::Image,
        "image/x-canon-cr2" => EntryDataType::Image,
        "image/tiff" => EntryDataType::Image,
        "image/bmp" => EntryDataType::Image,
        "image/heif" => EntryDataType::Image,
        "image/avif" => EntryDataType::Image,

        // Video
        "video/mp4" => EntryDataType::Video,
        "video/x-m4v" => EntryDataType::Video,
        "video/x-msvideo" => EntryDataType::Video,
        "video/quicktime" => EntryDataType::Video,
        "video/mpeg" => EntryDataType::Video,
        "video/webm" => EntryDataType::Video,
        "video/x-matroska" => EntryDataType::Video,
        "video/x-flv" => EntryDataType::Video,
        "video/x-ms-wmv" => EntryDataType::Video,

        // Audio
        "audio/midi" => EntryDataType::Audio,
        "audio/mpeg" => EntryDataType::Audio,
        "audio/m4a" => EntryDataType::Audio,
        "audio/x-wav" => EntryDataType::Audio,
        "audio/ogg" => EntryDataType::Audio,
        "audio/x-flac" => EntryDataType::Audio,
        "audio/aac" => EntryDataType::Audio,
        "audio/x-aiff" => EntryDataType::Audio,
        "audio/x-dsf" => EntryDataType::Audio,
        "audio/x-ape" => EntryDataType::Audio,

        mime_type => {
            // log::debug!(
            //     "datatype_from_mimetype trying to infer {:?} {:?} {}",
            //     data_path,
            //     file_path,
            //     mime_type
            // );
            // Catch text and dataframe types from file extension
            if is_tabular_from_extension(data_path, file_path) {
                EntryDataType::Tabular
            } else if mime_type.starts_with("text/") {
                EntryDataType::Text
            } else {
                // split on the first half of the mime type to fall back to audio, video, image
                let mime_type = mime_type.split('/').next().unwrap_or("");
                match mime_type {
                    "audio" => EntryDataType::Audio,
                    "video" => EntryDataType::Video,
                    "image" => EntryDataType::Image,
                    _ => EntryDataType::Binary,
                }
            }
        }
    }
}

pub fn file_data_type(path: &Path) -> EntryDataType {
    let mimetype = file_mime_type(path);
    datatype_from_mimetype(path, mimetype.as_str())
}

pub fn file_extension(path: &Path) -> String {
    match path.extension() {
        Some(extension) => match extension.to_str() {
            Some(ext) => ext.to_string(),
            None => "".to_string(),
        },
        None => "".to_string(),
    }
}

pub fn contains_ext(path: &Path, exts: &HashSet<String>) -> bool {
    match path.extension() {
        Some(extension) => match extension.to_str() {
            Some(ext) => exts.contains(ext.to_lowercase().as_str()),
            None => false,
        },
        None => false,
    }
}

pub fn has_ext(path: &Path, ext: &str) -> bool {
    match path.extension() {
        Some(extension) => extension == ext,
        None => false,
    }
}

pub fn replace_file_name_keep_extension(path: &Path, new_filename: String) -> PathBuf {
    let mut result = path.to_owned();
    result.set_file_name(new_filename);
    if let Some(extension) = path.extension() {
        result.set_extension(extension);
    }
    result
}

// recursive count files with extension
pub fn rcount_files_with_extension(dir: &Path, exts: &HashSet<String>) -> usize {
    let mut count = 0;
    if !dir.is_dir() {
        return count;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if contains_ext(&path, exts) {
                    count += 1
                }
            }
            Err(err) => {
                eprintln!("recursive_files_with_extensions Could not iterate over dir... {err}")
            }
        }
    }
    count
}

pub fn recursive_files_with_extensions(dir: &Path, exts: &HashSet<String>) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if contains_ext(&path, exts) {
                    files.push(path);
                }
            }
            Err(err) => {
                eprintln!("recursive_files_with_extensions Could not iterate over dir... {err}")
            }
        }
    }
    files
}

pub fn recursive_eligible_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    let mut mod_idx = 10;
    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // if it's not the hidden oxen dir and is not a directory
                // if !is_in_oxen_hidden_dir(&path) && !path.is_dir() {
                if !path.is_dir() {
                    files.push(path);

                    if files.len() % mod_idx == 0 {
                        log::debug!("Got {} files", files.len());
                        mod_idx *= 2;
                    }
                }
            }
            Err(err) => {
                eprintln!("recursive_files_with_extensions Could not iterate over dir... {err}")
            }
        }
    }
    files
}

pub fn count_files_in_dir(dir: &Path) -> usize {
    p_count_files_in_dir_w_progress(dir, None)
}

pub fn count_files_in_dir_w_progress(dir: &Path) -> usize {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message("🐂 Counting files...".to_string());
    p_count_files_in_dir_w_progress(dir, Some(pb))
}

pub fn p_count_files_in_dir_w_progress(dir: &Path, pb: Option<ProgressBar>) -> usize {
    let mut count: usize = 0;
    if dir.is_dir() {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if !is_in_oxen_hidden_dir(&path) && path.is_file() {
                                count += 1;
                                if let Some(ref pb) = pb {
                                    pb.set_message(format!("🐂 Found {:?} files", count));
                                }
                            }
                        }
                        Err(err) => log::warn!("error reading dir entry: {}", err),
                    }
                }
            }
            Err(err) => log::warn!("error reading dir: {}", err),
        }
    }
    count
}

pub fn count_files_in_dir_with_progress(dir: impl AsRef<Path>) -> usize {
    let dir = dir.as_ref();
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message("🐂 Counting files...".to_string());

    // TODO: Can we count in parallel by parallel walking dir?
    let mut count: usize = 0;
    if dir.is_dir() {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if !is_in_oxen_hidden_dir(&path) && path.is_file() {
                                count += 1;
                                pb.set_message(format!(
                                    "🐂 dir {:?} has {} files...",
                                    dir.file_name(),
                                    count
                                ))
                            }
                        }
                        Err(err) => log::warn!("error reading dir entry: {}", err),
                    }
                }
            }
            Err(err) => log::warn!("error reading dir: {}", err),
        }
    }
    count
}

pub fn count_items_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if dir.is_dir() {
        match std::fs::read_dir(dir) {
            Ok(entries) => {
                for entry in entries {
                    match entry {
                        Ok(entry) => {
                            let path = entry.path();
                            if !is_in_oxen_hidden_dir(&path) {
                                count += 1;
                            }
                        }
                        Err(err) => log::warn!("error reading dir entry: {}", err),
                    }
                }
            }
            Err(err) => log::warn!("error reading dir: {}", err),
        }
    }
    count
}

pub fn rcount_files_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if !dir.is_dir() {
        return count;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // if it's not the hidden oxen dir and is not a directory
                if !is_in_oxen_hidden_dir(&path) && !path.is_dir() {
                    // log::debug!("Found file {count}: {:?}", path);
                    count += 1;
                }
            }
            Err(err) => eprintln!("rcount_files_in_dir Could not iterate over dir... {err}"),
        }
    }
    count
}

pub fn rlist_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                // if it's not the hidden oxen dir and is not a directory
                if !is_in_oxen_hidden_dir(&path) && !path.is_dir() {
                    // log::debug!("Found file {count}: {:?}", path);
                    files.push(path);
                }
            }
            Err(err) => eprintln!("rcount_files_in_dir Could not iterate over dir... {err}"),
        }
    }
    files
}

pub fn is_in_oxen_hidden_dir(path: &Path) -> bool {
    for component in path.components() {
        if let Some(path_str) = component.as_os_str().to_str() {
            if path_str.eq(constants::OXEN_HIDDEN_DIR) {
                return true;
            }
        }
    }
    false
}

pub fn is_canonical(path: impl AsRef<Path>) -> Result<bool, OxenError> {
    let path = path.as_ref();
    let canon_path = canonicalize(path)?;

    if path == canon_path {
        log::debug!("path {path:?} IS canonical");
        return Ok(true);
    }

    log::debug!("path {path:?} is NOT canonical");
    Ok(false)
}

// Return canonicalized path if possible, otherwise return original
pub fn canonicalize(path: impl AsRef<Path>) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    //log::debug!("Path to canonicalize: {path:?}");
    match dunce::canonicalize(path) {
        Ok(canon_path) => Ok(canon_path),
        Err(err) => Err(OxenError::basic_str(format!(
            "path {path:?} cannot be canonicalized due to err {err:?}"
        ))),
    }
}

// Get the full path of a non-canonical parent from a canonical child path
pub fn full_path_from_child_path(
    parent: impl AsRef<Path>,
    child: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let parent_path = parent.as_ref();
    let child_path = child.as_ref();

    let parent_stem = stem_from_canonical_child_path(parent_path, child_path)?;
    Ok(parent_stem.join(parent_path))
}

// Find the stem that completes the absolute path of the parent from its child
// This is useful to find the absolute path of a repo when directly canonicalizing its path isn't possible, and we're calling add with an absolute path
// If the file is under the oxen control of the repo, then this will recover the necessary stem to get the correct canonical path to that repo
fn stem_from_canonical_child_path(
    parent_path: impl AsRef<Path>,
    child_path: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let parent_path = parent_path.as_ref();
    let child_path = child_path.as_ref();

    let child_components: Vec<Component> = child_path.components().collect();
    let parent_components: Vec<Component> = parent_path.components().collect();

    let relative_path = path_relative_to_dir(child_path, parent_path)?;
    let relative_components: Vec<Component> = relative_path.components().collect();

    if child_components.len() < parent_components.len() + relative_components.len() {
        return Err(OxenError::basic_str(format!(
            "Invalid path relationship: child path {:?} is not under parent path {:?}",
            child_path, parent_path
        )));
    }

    let ending_index = child_components.len() - relative_components.len() - parent_components.len();
    let path_slice = &child_components[..ending_index];
    let result: PathBuf = path_slice.iter().collect();
    Ok(result)
}

pub fn path_relative_to_dir(
    path: impl AsRef<Path>,
    dir: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let dir = dir.as_ref();

    // log::debug!("path_relative_to_dir starting path: {path:?}");
    // log::debug!("path_relative_to_dir staring dir: {dir:?}");

    // Split paths into components
    let path_components: Vec<Component> = path.components().collect();
    let dir_components: Vec<Component> = dir.components().collect();

    if path_components.is_empty() || dir == path {
        return Ok(PathBuf::new());
    }

    if dir_components.is_empty() || dir_components.len() > path_components.len() {
        // Iterate through the components instead of returning original to normalize path for windows
        let mut result = PathBuf::new();
        let path_slice = &path_components;

        // Adjust for special paths like '.', '..', etc
        for component in path_slice.iter() {
            if matches!(component, Component::Normal(_)) {
                result.push::<&Path>(component.as_ref());
            }
        }

        return Ok(result);
    }

    // Get iterators for the component vectors
    let mut path_iter = path_components.iter();
    let mut dir_iter = dir_components.iter();
    let starting_dir_iter = dir_iter.clone();

    let mut dir_component = dir_iter.next().unwrap();
    let mut matches = 0;
    let mut start_index = 0;

    for _ in 0..(path_components.len()) {
        let path_component = path_iter.next().expect("Path bounds violated");
        let path_str = path_component.as_os_str();
        let dir_str = dir_component.as_os_str();

        if path_str == dir_str {
            matches += 1;
            if matches == dir_components.len() {
                let mut result = PathBuf::new();
                let path_slice = &path_components[(start_index + 1)..];

                // Adjust for special paths like '.', '..', etc
                for component in path_slice.iter() {
                    if matches!(component, Component::Normal(_)) {
                        result.push::<&Path>(component.as_ref());
                    }
                }
                // log::debug!("result: {result:?}");
                return Ok(result);
            }
            start_index += 1;
            dir_component = dir_iter.next().expect("Dir bounds violated");
            continue;
        }

        // Check length first as an optimization
        if path_str.len() == dir_str.len() {
            // On Windows, if the components don't match, it may be because of casing inconsistency
            // So, if the raw components don't match, convert them to lowercase strings
            let path_lower = path_str.to_string_lossy().to_lowercase();
            let dir_lower = dir_str.to_string_lossy().to_lowercase();

            if path_lower == dir_lower {
                matches += 1;
                if matches == dir_components.len() {
                    let mut result = PathBuf::new();
                    let path_slice = &path_components[(start_index + 1)..];

                    for component in path_slice.iter() {
                        if matches!(component, Component::Normal(_)) {
                            result.push::<&Path>(component.as_ref());
                        }
                    }
                    // log::debug!("result: {result:?}");
                    return Ok(result);
                }
                start_index += 1;
                dir_component = dir_iter.next().expect("Dir bounds violated");
                continue;
            }
        }

        // If the components don't match, reset dir_iter and dir_component
        dir_iter = starting_dir_iter.clone();
        dir_component = dir_iter.next().unwrap();
        start_index += 1;
    }

    // If the loop finishes, the path cannot be found relative to the dir
    // Returning the original path is the expected behavior
    let mut result = PathBuf::new();
    let path_slice = &path_components;

    for component in path_slice.iter() {
        result.push::<&Path>(component.as_ref());
    }
    Ok(result)
}

pub fn linux_path_str(string: &str) -> String {
    // Convert string to bytes, replacing '\\' with '/' if necessary
    let bytes = string.as_bytes();
    let mut new_bytes: Vec<u8> = Vec::new();
    for byte in bytes.iter() {
        if *byte == b'\\' {
            new_bytes.push(b'/');
        } else {
            new_bytes.push(*byte);
        }
    }
    String::from_utf8(new_bytes).unwrap()
}

pub fn linux_path(path: &Path) -> PathBuf {
    // Convert string to bytes, replacing '\\' with '/' if necessary
    let string = path.to_str().unwrap();
    Path::new(&linux_path_str(string)).to_path_buf()
}

pub fn disk_usage_for_path(path: &Path) -> Result<DiskUsage, OxenError> {
    log::debug!("disk_usage_for_path: {:?}", path);
    let disks = sysinfo::Disks::new_with_refreshed_list();

    if disks.is_empty() {
        return Err(OxenError::basic_str("No disks found"));
    }

    // try to choose the disk that the path is on
    let mut selected_disk = disks.first().unwrap();
    for disk in &disks {
        let disk_mount_len = disk.mount_point().to_str().unwrap_or_default().len();
        let selected_disk_mount_len = selected_disk
            .mount_point()
            .to_str()
            .unwrap_or_default()
            .len();

        // pick the disk with the longest mount point that is a prefix of the path
        if path.starts_with(disk.mount_point()) && disk_mount_len > selected_disk_mount_len {
            selected_disk = disk;
            break;
        }
    }

    log::debug!("disk_usage_for_path selected disk: {:?}", selected_disk);
    let total_gb = selected_disk.total_space() as f64 / bytesize::GB as f64;
    let free_gb = selected_disk.available_space() as f64 / bytesize::GB as f64;
    let used_gb = total_gb - free_gb;
    let percent_used = used_gb / total_gb;

    Ok(DiskUsage {
        total_gb,
        used_gb,
        free_gb,
        percent_used,
    })
}
pub fn is_any_parent_in_set(file_path: &Path, path_set: &HashSet<PathBuf>) -> bool {
    let mut current_path = file_path.to_path_buf();
    // Iterate through parent directories
    log::debug!("checking if {:?} is in {:?}", current_path, path_set);
    while let Some(parent) = current_path.parent() {
        log::debug!("checking if {:?} is in {:?}", current_path, path_set);
        if path_set.contains(parent) {
            return true;
        }
        current_path = parent.to_path_buf()
    }

    false
}

pub fn open_file(path: impl AsRef<Path>) -> Result<File, OxenError> {
    match File::open(path.as_ref()) {
        Ok(file) => Ok(file),
        Err(err) => Err(OxenError::basic_str(format!(
            "Failed to open file: {:?}\n{:?}",
            path.as_ref(),
            err
        ))),
    }
}

fn detect_image_format_from_version(
    versioned_store: Arc<dyn VersionStore>,
    hash: &str,
) -> Result<ImageFormat, OxenError> {
    let mut file = versioned_store.open_version(hash)?;

    let mut header = [0u8; 10];
    let n = file.read(&mut header).map_err(|e| {
        OxenError::basic_str(format!("Failed to read version file {}: {}", hash, e))
    })?;
    // Set the pointer back to the front
    file.seek(SeekFrom::Start(0)).map_err(|e| {
        OxenError::basic_str(format!("Failed to seek version file {}: {}", hash, e))
    })?;

    // detect format
    let format = image::guess_format(&header[..n])
        .map_err(|_| OxenError::basic_str(format!("Unknown image format for version: {}", hash)))?;

    Ok(format)
}

// Caller must provide out path because it differs between remote staged vs. committed files
pub fn resize_cache_image_version_store(
    version_store: Arc<dyn VersionStore>,
    img_hash: &str,
    image_path: &Path,
    resize_path: &Path,
    resize: ImgResize,
) -> Result<(), OxenError> {
    log::debug!("resize to path {:?} from {:?}", resize_path, image_path);
    if resize_path.exists() {
        return Ok(());
    }

    let image_format = detect_image_format_from_version(Arc::clone(&version_store), img_hash);
    let img = match image_format {
        Ok(format) => {
            let reader = version_store.open_version(img_hash)?;
            image::load(BufReader::new(reader), format)?
        }
        Err(_) => {
            log::debug!("Could not detect image format, opening file without format");
            let reader = version_store.open_version(img_hash)?;

            ImageReader::new(BufReader::new(reader))
                .with_guessed_format()?
                .decode()?
        }
    };

    let resized_img = if resize.width.is_some() && resize.height.is_some() {
        img.resize_exact(
            resize.width.unwrap(),
            resize.height.unwrap(),
            image::imageops::FilterType::Lanczos3,
        )
    } else if resize.width.is_some() {
        img.resize(
            resize.width.unwrap(),
            resize.width.unwrap(),
            image::imageops::FilterType::Lanczos3,
        )
    } else if resize.height.is_some() {
        img.resize(
            resize.height.unwrap(),
            resize.height.unwrap(),
            image::imageops::FilterType::Lanczos3,
        )
    } else {
        img
    };
    log::debug!("about to save {:?}", resize_path);

    let resize_parent = resize_path.parent().unwrap_or(Path::new(""));
    if !resize_parent.exists() {
        util::fs::create_dir_all(resize_parent)?;
    }

    resized_img.save(resize_path).unwrap();
    log::debug!("saved {:?}", resize_path);
    Ok(())
}

pub fn to_unix_str(path: impl AsRef<Path>) -> String {
    path.as_ref()
        .to_str()
        .unwrap_or_default()
        .replace('\\', "/")
}

pub fn is_glob_path(path: impl AsRef<Path>) -> bool {
    let glob_chars = ['*', '?', '[', ']'];
    glob_chars
        .iter()
        .any(|c| path.as_ref().to_str().unwrap_or_default().contains(*c))
}

pub fn remove_paths(src: &Path) -> Result<(), OxenError> {
    if src.is_dir() {
        log::debug!("Calling remove_dir_all: {src:?}");
        remove_dir_all(src)
    } else {
        log::debug!("Calling remove_file: {src:?}");
        remove_file(src)
    }
}

pub fn is_modified_from_node(path: &Path, node: &FileNode) -> Result<bool, OxenError> {
    // First, check if the file exists; return false if not
    if !path.exists() {
        log::debug!("is_modified_from_node found non-existant path {path:?}. Returning false");
        return Ok(false);
    }

    // Second, check the length of the file
    let meta = util::fs::metadata(path)?;

    let file_size = meta.len();
    let node_size = node.num_bytes();

    if file_size != node_size {
        return Ok(true);
    }

    // Third, check the last modified times
    let file_last_modified = FileTime::from_last_modification_time(&meta);
    let node_last_modified = util::fs::last_modified_time(
        node.last_modified_seconds(),
        node.last_modified_nanoseconds(),
    );

    if file_last_modified == node_last_modified {
        return Ok(false);
    }

    // Finally, check the hashes
    let node_hash = node.hash().to_u128();
    let working_hash = util::hasher::get_hash_given_metadata(path, &meta)?;

    if node_hash == working_hash {
        Ok(false)
    } else {
        Ok(true)
    }
}

// Only uses the metadata to check for modification
pub fn is_modified_metadata_only(path: &Path, node: &FileNode) -> Result<bool, OxenError> {
    // First, check if the file exists; return false if not
    if !path.exists() {
        log::debug!("is_modified_from_node found non-existant path {path:?}. Returning false");
        return Ok(false);
    }

    // Second, check the length of the file
    let meta = util::fs::metadata(path)?;

    let file_size = meta.len();
    let node_size = node.num_bytes();

    if file_size != node_size {
        return Ok(true);
    }

    // Third, check the last modified times
    let file_last_modified = FileTime::from_last_modification_time(&meta);
    let node_last_modified = util::fs::last_modified_time(
        node.last_modified_seconds(),
        node.last_modified_nanoseconds(),
    );

    if file_last_modified == node_last_modified {
        return Ok(false);
    }

    Ok(true)
}

// Calculate a node's last modified time

pub fn last_modified_time(last_modified_seconds: i64, last_modified_nanoseconds: u32) -> FileTime {
    let node_modified_nanoseconds = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(last_modified_seconds as u64)
        + std::time::Duration::from_nanos(last_modified_nanoseconds as u64);

    FileTime::from_system_time(node_modified_nanoseconds)
}
#[cfg(test)]
mod tests {
    use crate::constants::{self, VERSION_FILE_NAME};
    use crate::error::OxenError;
    use crate::model::{CommitEntry, EntryDataType};
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other").join("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_2_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_3_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("runs")
            .join("54321")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("runs").join("54321");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn full_file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data")
            .join("test")
            .join("other")
            .join("file.txt");
        let dir = Path::new("data").join("test").join("other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_another_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other").join("dir");
        let dir = Path::new("data").join("test");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other").join("dir"));

        Ok(())
    }

    #[test]
    fn path_relative_to_unrelated_dir() -> Result<(), OxenError> {
        let file = Path::new("data").join("test").join("other").join("dir");
        let dir = Path::new("some").join("repo");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(
            relative,
            Path::new("data").join("test").join("other").join("dir")
        );

        Ok(())
    }

    #[tokio::test]
    async fn version_path() -> Result<(), OxenError> {
        test::run_empty_local_repo_test(|repo| {
            let entry = CommitEntry {
                commit_id: String::from("1234"),
                path: PathBuf::from("hello_world.txt"),
                hash: String::from("59E029D4812AEBF0"), // dir structure -> 59/E029D4812AEBF0
                num_bytes: 0,
                last_modified_seconds: 0,
                last_modified_nanoseconds: 0,
            };
            let path = util::fs::version_path(&repo, &entry);
            let versions_dir = util::fs::oxen_hidden_dir(&repo.path).join(constants::VERSIONS_DIR);
            let relative_path = util::fs::path_relative_to_dir(path, versions_dir)?;
            assert_eq!(
                relative_path,
                Path::new(constants::FILES_DIR)
                    .join("59")
                    .join(Path::new("E029D4812AEBF0"))
                    .join(Path::new(VERSION_FILE_NAME))
            );

            Ok(())
        })
    }

    #[tokio::test]
    async fn detect_file_type() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
            let python_file = "add_1.py";
            let python_with_interpreter_file = "add_2.py";

            test::write_txt_file_to_path(
                repo.path.join(python_file),
                r"import os


def add(a, b):
    return a + b",
            )?;

            test::write_txt_file_to_path(
                repo.path.join(python_with_interpreter_file),
                r"#!/usr/bin/env python3
import os


def add(a, b):
    return a + b",
            )?;

            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&repo.path.join(python_file))
            );

            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&repo.path.join(python_with_interpreter_file))
            );

            assert_eq!(
                EntryDataType::Tabular,
                util::fs::file_data_type(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("bounding_box.csv")
                )
            );
            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt")
                )
            );

            let test_id_file = repo.path.join("test_id.txt");
            let test_id_file_no_ext = repo.path.join("test_id");
            util::fs::copy(
                Path::new("data")
                    .join("test")
                    .join("text")
                    .join("test_id.txt"),
                &test_id_file,
            )?;
            util::fs::copy(
                Path::new("data")
                    .join("test")
                    .join("text")
                    .join("test_id.txt"),
                &test_id_file_no_ext,
            )?;

            assert_eq!(EntryDataType::Text, util::fs::file_data_type(&test_id_file));
            assert_eq!(
                EntryDataType::Text,
                util::fs::file_data_type(&test_id_file_no_ext)
            );
            assert_eq!(
                EntryDataType::Image,
                util::fs::file_data_type(&repo.path.join("test").join("1.jpg"))
            );

            Ok(())
        })
    }

    #[test]
    fn detect_file_type_json_array() -> Result<(), OxenError> {
        test::run_empty_dir_test(|_| {
            assert_eq!(
                EntryDataType::Tabular,
                util::fs::file_data_type(
                    &Path::new("data")
                        .join("test")
                        .join("json")
                        .join("tabular.json")
                )
            );

            Ok(())
        })
    }

    #[test]
    fn replace_file_name_keep_extension_no_extension() -> Result<(), OxenError> {
        let prior_path = Path::new("adjfkaljeklwjkljdaklfd.txt");
        let prior_path_no_extension = Path::new("bdsfadfklajfkelj");
        let prior_path_arbitrary = Path::new("jdakfljdfskl.boom");

        let new_filename = "data".to_string();
        assert_eq!(
            util::fs::replace_file_name_keep_extension(prior_path, new_filename.clone()),
            Path::new("data.txt")
        );

        assert_eq!(
            util::fs::replace_file_name_keep_extension(
                prior_path_no_extension,
                new_filename.clone()
            ),
            Path::new("data")
        );

        assert_eq!(
            util::fs::replace_file_name_keep_extension(prior_path_arbitrary, new_filename),
            Path::new("data.boom")
        );

        Ok(())
    }

    #[test]
    fn to_unix_str() {
        assert_eq!(
            util::fs::to_unix_str(Path::new("data\\test\\file.txt")),
            "data/test/file.txt"
        );
    }
}
