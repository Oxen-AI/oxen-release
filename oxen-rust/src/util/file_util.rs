use jwalk::WalkDir;
use std::collections::HashSet;
use std::fs;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;

pub struct FileUtil {}

impl FileUtil {
    pub fn read_from_path(path: &Path) -> String {
        let mut result = String::from("");
        match fs::read_to_string(path) {
            Ok(contents) => {
                result = contents;
            }
            Err(_) => {
                eprintln!("Could not open staging file {}", path.display())
            }
        }
        result
    }

    pub fn write_to_path(path: &Path, value: &str) {
        match File::create(path) {
            Ok(mut file) => match file.write(value.as_bytes()) {
                Ok(_) => {}
                Err(err) => {
                    eprintln!("Could not write file {:?}\n{}", path, err)
                }
            },
            Err(err) => {
                eprintln!("Could not create file {:?}\n{}", path, err)
            }
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

    pub fn read_lines(path: &Path) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        match File::open(&path) {
            Ok(file) => lines = FileUtil::read_lines_file(&file),
            Err(_) => {
                eprintln!("Could not open staging file {}", path.display())
            }
        }
        lines
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
                    "FileUtil::list_files_in_dir Could not find dir: {} err: {}",
                    dir.display(),
                    err
                )
            }
        }

        files
    }

    pub fn is_image(path: &Path) -> bool {
        let exts: HashSet<String> = vec!["jpg", "png"].into_iter().map(String::from).collect();
        FileUtil::contains_ext(path, &exts)
    }

    pub fn is_text(path: &Path) -> bool {
        let exts: HashSet<String> = vec!["txt"].into_iter().map(String::from).collect();
        FileUtil::contains_ext(path, &exts)
    }

    pub fn is_video(path: &Path) -> bool {
        let exts: HashSet<String> = vec!["mp4"].into_iter().map(String::from).collect();
        FileUtil::contains_ext(path, &exts)
    }

    pub fn is_audio(path: &Path) -> bool {
        let exts: HashSet<String> = vec!["mp3", "wav"].into_iter().map(String::from).collect();
        FileUtil::contains_ext(path, &exts)
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

    pub fn recursive_files_with_extensions(dir: &Path, exts: &HashSet<String>) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = Vec::new();
        for entry in WalkDir::new(dir) {
            match entry {
                Ok(val) => {
                    let path = val.path();
                    if FileUtil::contains_ext(&path, exts) {
                        files.push(path);
                    }
                }
                Err(err) => eprintln!("Could not iterate over dir... {}", err),
            }
        }
        files
    }
}
