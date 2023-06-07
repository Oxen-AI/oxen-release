//! Wrapper around std::fs commands to make them easier to use
//! and eventually abstract away the fs implementation
//!

use jwalk::WalkDir;

use bytesize;
use simdutf8::compat::from_utf8;
use std::collections::HashSet;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use sysinfo::{DiskExt, System, SystemExt};

use crate::constants;
use crate::constants::CACHE_DIR;
use crate::constants::CONTENT_IS_VALID;
use crate::constants::DATA_ARROW_FILE;
use crate::constants::HISTORY_DIR;
use crate::error::OxenError;
use crate::model::Commit;
use crate::model::{CommitEntry, EntryDataType, LocalRepository};
use crate::view::health::DiskUsage;
use crate::{api, util};

pub fn oxen_hidden_dir(repo_path: impl AsRef<Path>) -> PathBuf {
    PathBuf::from(repo_path.as_ref()).join(Path::new(constants::OXEN_HIDDEN_DIR))
}

pub fn oxen_home_dir() -> Result<PathBuf, OxenError> {
    match dirs::home_dir() {
        Some(home_dir) => Ok(home_dir.join(constants::OXEN_HIDDEN_DIR)),
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
    match api::local::commits::get_by_id(repo, commit_id)? {
        Some(commit) => match api::local::entries::get_commit_entry(repo, &commit, filepath)? {
            Some(entry) => {
                let path = version_path(repo, &entry);
                let arrow_path = path.parent().unwrap().join(DATA_ARROW_FILE);
                if arrow_path.exists() {
                    Ok(arrow_path)
                } else {
                    Ok(path)
                }
            }
            None => Err(OxenError::path_does_not_exist(filepath.to_path_buf())),
        },
        None => Err(OxenError::committish_not_found(commit_id.into())),
    }
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
        return Err(OxenError::entry_does_not_exist(version_path));
    }
    let meta = util::fs::metadata(&version_path)?;
    Ok(meta.len())
    // }
}

pub fn version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    version_path_from_hash_and_file(&repo.path, entry.hash.clone(), entry.filename())
}

pub fn version_path_from_dst(dst: impl AsRef<Path>, entry: &CommitEntry) -> PathBuf {
    version_path_from_hash_and_file(dst, entry.hash.clone(), entry.filename())
}

pub fn df_version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    let version_dir = version_dir_from_hash(&repo.path, entry.hash.clone());
    version_dir.join(DATA_ARROW_FILE)
}

pub fn version_path_from_hash_and_file(
    dst: impl AsRef<Path>,
    hash: String,
    filename: PathBuf,
) -> PathBuf {
    let version_dir = version_dir_from_hash(dst, hash);
    version_dir.join(filename)
}

pub fn version_dir_from_hash(dst: impl AsRef<Path>, hash: String) -> PathBuf {
    let topdir = &hash[..2];
    let subdir = &hash[2..];
    oxen_hidden_dir(dst.as_ref())
        .join(constants::VERSIONS_DIR)
        .join(constants::FILES_DIR)
        .join(topdir)
        .join(subdir)
}

pub fn read_from_path(path: &Path) -> Result<String, OxenError> {
    match std::fs::read_to_string(path) {
        Ok(contents) => Ok(contents),
        Err(_) => {
            let err = format!(
                "util::fs::read_from_path could not open: {}",
                path.display()
            );
            log::error!("{}", err);
            Err(OxenError::basic_str(&err))
        }
    }
}

pub fn write_to_path(path: &Path, value: &str) -> Result<(), OxenError> {
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

pub fn read_lines_file(file: &File) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    let reader = BufReader::new(file);
    for line in reader.lines().flatten() {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            lines.push(String::from(trimmed));
        }
    }
    lines
}

pub fn read_first_line<P: AsRef<Path>>(path: P) -> Result<String, OxenError> {
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

pub fn count_lines(path: impl AsRef<Path>) -> Result<usize, std::io::Error> {
    let file = File::open(path)?;
    p_count_lines(file)
}

fn p_count_lines<R: std::io::Read>(handle: R) -> Result<usize, std::io::Error> {
    let mut reader = BufReader::with_capacity(1024 * 32, handle);
    let mut count = 1;
    loop {
        let len = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            count += bytecount::count(&buf, b'\n');
            buf.len()
        };
        reader.consume(len);
    }
    Ok(count)
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

pub fn list_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = Vec::new();
    match std::fs::read_dir(dir) {
        Ok(paths) => {
            for path in paths.flatten() {
                if util::fs::metadata(path.path()).unwrap().is_file() {
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

pub fn rlist_files_in_dir(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = vec![];
    if !dir.is_dir() {
        return files;
    }

    for entry in WalkDir::new(dir) {
        match entry {
            Ok(val) => {
                let path = val.path();
                if path.is_file() {
                    files.push(path);
                }
            }
            Err(err) => eprintln!("rlist_files_in_dir Could not iterate over dir... {err}"),
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
    if path.join(".oxen").exists() {
        return Some(path.to_path_buf());
    }

    if let Some(parent) = path.parent() {
        get_repo_root(parent)
    } else {
        None
    }
}

pub fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), OxenError> {
    std::fs::create_dir_all(&dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            std::fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
}

/// Wrapper around the std::fs::copy command to tell us which file failed to copy
pub fn copy(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    let dst = dst.as_ref();
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

/// Wrapper around the std::fs::create_dir_all command to tell us which file it failed on
pub fn create_dir_all(src: impl AsRef<Path>) -> Result<(), OxenError> {
    let src = src.as_ref();
    match std::fs::create_dir_all(src) {
        Ok(_) => Ok(()),
        Err(err) => {
            log::error!("{}", err);
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
            log::error!("{}", err);
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
            log::error!("{}", err);
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
            log::error!("{}", err);
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
            log::error!("{}", err);
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
            log::error!("{}", err);
            Err(OxenError::file_create_error(path, err))
        }
    }
}

pub fn is_tabular(path: &Path) -> bool {
    let exts: HashSet<String> = vec!["csv", "tsv", "parquet", "arrow", "ndjson", "jsonl"]
        .into_iter()
        .map(String::from)
        .collect();
    contains_ext(path, &exts)
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
    if let Ok(line) = read_first_line(path) {
        from_utf8(line.as_bytes()).is_ok()
    } else {
        false
    }
}

pub fn file_mime_type(path: &Path) -> String {
    match infer::get_from_path(path) {
        Ok(Some(kind)) => String::from(kind.mime_type()),
        _ => {
            if is_markdown(path) {
                String::from("text/markdown")
            } else if is_utf8(path) {
                String::from("text/plain")
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

pub fn datatype_from_mimetype(path: &Path, mimetype: &str) -> EntryDataType {
    match mimetype {
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
        "audio/ogg" => EntryDataType::Audio,
        "audio/x-flac" => EntryDataType::Audio,
        "audio/aac" => EntryDataType::Audio,
        "audio/x-aiff" => EntryDataType::Audio,
        "audio/x-dsf" => EntryDataType::Audio,
        "audio/x-ape" => EntryDataType::Audio,

        _ => {
            // Catch text and dataframe types from file extension
            if is_tabular(path) {
                EntryDataType::Tabular
            } else if "text/plain" == mimetype || "text/markdown" == mimetype {
                EntryDataType::Text
            } else {
                EntryDataType::Binary
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

pub fn path_relative_to_dir(
    path: impl AsRef<Path>,
    dir: impl AsRef<Path>,
) -> Result<PathBuf, OxenError> {
    let path = path.as_ref();
    let dir = dir.as_ref();

    let mut mut_path = path.to_path_buf();
    let mut components: Vec<PathBuf> = vec![];
    while mut_path.parent().is_some() {
        // println!("Comparing {:?} => {:?} => {:?}", path, mut_path.parent(), dir);
        if let Some(filename) = mut_path.file_name() {
            if mut_path != dir {
                components.push(PathBuf::from(filename));
            } else {
                break;
            }
        }

        mut_path.pop();
    }
    components.reverse();

    let mut result = PathBuf::new();
    for component in components.iter() {
        result = result.join(component);
    }

    Ok(result)
}

pub fn disk_usage_for_path(path: &Path) -> Result<DiskUsage, OxenError> {
    log::debug!("disk_usage_for_path: {:?}", path);
    let mut sys = System::new();
    sys.refresh_disks_list();

    if sys.disks().is_empty() {
        return Err(OxenError::basic_str("No disks found"));
    }

    // try to choose the disk that the path is on
    let mut selected_disk = sys.disks().first().unwrap();
    for disk in sys.disks() {
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

#[cfg(test)]
mod tests {
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::{CommitEntry, EntryDataType};
    use crate::test;
    use crate::util;

    use std::path::{Path, PathBuf};

    #[test]
    fn file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/file.txt");
        let dir = Path::new("data/test/");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other/file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_2_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/file.txt");
        let dir = Path::new("data/test/other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_3_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/runs/54321/file.txt");
        let dir = Path::new("data/test/runs/54321");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn full_file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("/tmp/data/test/other/file.txt");
        let dir = Path::new("/tmp/data/test/other");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other");
        let dir = Path::new("data/test/");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_another_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/dir");
        let dir = Path::new("data/test/");

        let relative = util::fs::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other/dir"));

        Ok(())
    }

    #[test]
    fn version_path() -> Result<(), OxenError> {
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
                    .join(Path::new("1234.txt"))
            );

            Ok(())
        })
    }

    #[test]
    fn detect_file_type() -> Result<(), OxenError> {
        test::run_training_data_repo_test_no_commits(|repo| {
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
            util::fs::copy("data/test/text/test_id.txt", &test_id_file)?;
            util::fs::copy("data/test/text/test_id.txt", &test_id_file_no_ext)?;

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
}
