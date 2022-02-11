
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::fs::File;
use std::io::{BufReader};
use std::io::prelude::*;
use jwalk::{WalkDir};
use std::collections::HashSet;

pub struct FileUtil {

}

impl FileUtil {
    pub fn read_from_path(path: &Path) -> String {
        let mut result = String::from("");
        match fs::read_to_string(path) {
            Ok(contents) => {
                result = contents;
            },
            Err(_) => {
                eprintln!("Could not open staging file {}", path.display())
            }
        }
        result
    }

    pub fn read_lines_file(file: &File) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        let reader = BufReader::new(file);
        for line in reader.lines() {
            match line {
                Ok(valid) => {
                    let trimmed = valid.trim();
                    if !trimmed.is_empty() {
                        lines.push(String::from(trimmed));
                    }
                },
                Err(_) => {/* Couldnt read line */}
            }
        }
        lines
    }

    pub fn read_lines(path: &Path) -> Vec<String> {
        let mut lines: Vec<String> = Vec::new();
        match File::open(&path) {
            Ok(file) => {
                lines = FileUtil::read_lines_file(&file)
            },
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
                for path in paths {
                    match path {
                        Ok(val) => {
                            if fs::metadata(val.path()).unwrap().is_file() {
                                files.push(val.path());
                            }
                        }
                        Err(_) => {}
                    }
                }
            },
            Err(err) => {
                eprintln!("FileUtil::list_files_in_dir Could not find dir: {} err: {}", dir.display(), err)
            }
        }
        
        files
    }

    pub fn recursive_files_with_extensions(dir: &Path, exts: &HashSet<String>) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = Vec::new();
        for entry in WalkDir::new(dir) {
            match entry {
                Ok(val) => {
                    match val.path().extension() {
                        Some(extension) => {
                            match extension.to_str() {
                                Some(ext) => {
                                    if exts.contains(ext) {
                                        files.push(val.path());
                                    }
                                },
                                None => {
                                    eprintln!("Could not convert ext to string... {}", val.path().display())
                                }
                            }
    
                        },
                        None => {
                            // Ignore files with no extension
                        }
                    }
                },
                Err(err) => eprintln!("Could not iterate over dir... {}", err),
            }
        }
        files
    }
}
