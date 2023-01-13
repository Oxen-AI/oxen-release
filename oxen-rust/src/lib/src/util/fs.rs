use jwalk::WalkDir;

use simdutf8::compat::from_utf8;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::{fs, io};

use crate::api;
use crate::constants;
use crate::constants::DATA_ARROW_FILE;
use crate::error::OxenError;
use crate::model::{CommitEntry, LocalRepository, Schema};

pub fn oxen_hidden_dir(repo_path: &Path) -> PathBuf {
    PathBuf::from(&repo_path).join(Path::new(constants::OXEN_HIDDEN_DIR))
}

pub fn config_filepath(repo_path: &Path) -> PathBuf {
    oxen_hidden_dir(repo_path).join(constants::REPO_CONFIG_FILENAME)
}

pub fn repo_exists(repo_path: &Path) -> bool {
    oxen_hidden_dir(repo_path).exists()
}

pub fn schema_version_dir(repo: &LocalRepository, schema: &Schema) -> PathBuf {
    // .oxen/versions/schemas/SCHEMA_HASH
    oxen_hidden_dir(&repo.path)
        .join(constants::VERSIONS_DIR)
        .join(constants::SCHEMAS_DIR)
        .join(&schema.hash)
}

// NOTE: This was for CADF, was too inefficient for now
// pub fn schema_df_path(repo: &LocalRepository, schema: &Schema) -> PathBuf {
//     schema_version_dir(repo, schema).join(DATA_ARROW_FILE)
// }

pub fn version_path_for_commit_id(
    repo: &LocalRepository,
    commit_id: &str,
    filepath: &Path,
) -> Result<PathBuf, OxenError> {
    match api::local::commits::get_by_id(repo, commit_id)? {
        Some(commit) => match api::local::entries::get_entry_for_commit(repo, &commit, filepath)? {
            Some(entry) => {
                let path = version_path(repo, &entry);
                // if is_tabular(filepath) {
                //     let data_file = path.parent().unwrap().join(DATA_ARROW_FILE);
                //     Ok(data_file)
                // } else {
                Ok(path)
                // }
            }
            None => Err(OxenError::file_does_not_exist(filepath)),
        },
        None => Err(OxenError::commit_id_does_not_exist(commit_id)),
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
    //     let meta = std::fs::metadata(&data_file)?;
    //     Ok(meta.len())
    // } else {
    if !version_path.exists() {
        return Err(OxenError::file_does_not_exist(version_path));
    }
    let meta = std::fs::metadata(&version_path)?;
    Ok(meta.len())
    // }
}

pub fn version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    version_path_from_hash_and_file(repo, entry.hash.clone(), entry.filename())
}

pub fn df_version_path(repo: &LocalRepository, entry: &CommitEntry) -> PathBuf {
    let version_dir = version_dir_from_hash(repo, entry.hash.clone());
    version_dir.join(DATA_ARROW_FILE)
}

pub fn version_path_from_hash_and_file(
    repo: &LocalRepository,
    hash: String,
    filename: PathBuf,
) -> PathBuf {
    let version_dir = version_dir_from_hash(repo, hash);
    version_dir.join(filename)
}

pub fn version_dir_from_hash(repo: &LocalRepository, hash: String) -> PathBuf {
    let topdir = &hash[..2];
    let subdir = &hash[2..];
    oxen_hidden_dir(&repo.path)
        .join(constants::VERSIONS_DIR)
        .join(constants::FILES_DIR)
        .join(topdir)
        .join(subdir)
}

pub fn read_from_path(path: &Path) -> Result<String, OxenError> {
    match fs::read_to_string(path) {
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
                "Could not write file {:?}\n{}",
                path, err
            ))),
        },
        Err(err) => Err(OxenError::basic_str(format!(
            "Could not create file to write {:?}\n{}",
            path, err
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
            "Could not read line from file: {:?}",
            file
        )))
    }
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
    match fs::read_dir(dir) {
        Ok(paths) => {
            for path in paths.flatten() {
                if fs::metadata(path.path()).unwrap().is_file() {
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
            Err(err) => eprintln!("rlist_paths_in_dir Could not iterate over dir... {}", err),
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
            Err(err) => eprintln!("rlist_files_in_dir Could not iterate over dir... {}", err),
        }
    }
    files
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

pub fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
    fs::create_dir_all(&dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
        } else {
            fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
        }
    }
    Ok(())
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

pub fn file_datatype(path: &Path) -> String {
    log::debug!("Checking data type for path: {:?}", path);
    if is_markdown(path) {
        String::from("markdown")
    } else if is_image(path) {
        String::from("image")
    } else if is_video(path) {
        String::from("video")
    } else if is_audio(path) {
        String::from("audio")
    } else if is_tabular(path) {
        String::from("tabular")
    } else if is_utf8(path) {
        String::from("text")
    } else {
        String::from("unknown")
    }
}

pub fn contains_ext(path: &Path, exts: &HashSet<String>) -> bool {
    match path.extension() {
        Some(extension) => match extension.to_str() {
            Some(ext) => exts.contains(ext),
            None => false,
        },
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
            Err(err) => eprintln!(
                "recursive_files_with_extensions Could not iterate over dir... {}",
                err
            ),
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
            Err(err) => eprintln!(
                "recursive_files_with_extensions Could not iterate over dir... {}",
                err
            ),
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
            Err(err) => eprintln!(
                "recursive_files_with_extensions Could not iterate over dir... {}",
                err
            ),
        }
    }
    files
}

pub fn count_files_in_dir(dir: &Path) -> usize {
    let mut count: usize = 0;
    if dir.is_dir() {
        match fs::read_dir(dir) {
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
        match fs::read_dir(dir) {
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
                    count += 1;
                }
            }
            Err(err) => eprintln!("rcount_files_in_dir Could not iterate over dir... {}", err),
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

pub fn path_relative_to_dir(path: &Path, dir: &Path) -> Result<PathBuf, OxenError> {
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

    // println!("{:?}", components);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use crate::constants;
    use crate::error::OxenError;
    use crate::model::CommitEntry;
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
            let relative_path = util::fs::path_relative_to_dir(&path, &versions_dir)?;
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
                "tabular",
                util::fs::file_datatype(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("bounding_box.csv")
                )
            );
            assert_eq!(
                "text",
                util::fs::file_datatype(
                    &repo
                        .path
                        .join("annotations")
                        .join("train")
                        .join("annotations.txt")
                )
            );

            let test_id_file = repo.path.join("test_id.txt");
            let test_id_file_no_ext = repo.path.join("test_id");
            std::fs::copy("data/test/text/test_id.txt", &test_id_file)?;
            std::fs::copy("data/test/text/test_id.txt", &test_id_file_no_ext)?;

            assert_eq!("text", util::fs::file_datatype(&test_id_file));
            assert_eq!("text", util::fs::file_datatype(&test_id_file_no_ext));
            assert_eq!(
                "image",
                util::fs::file_datatype(&repo.path.join("test").join("1.jpg"))
            );

            Ok(())
        })
    }
}
