use jwalk::WalkDir;
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::{fs, io};

use crate::error::OxenError;

pub struct FileUtil {}

impl FileUtil {
    pub fn read_from_path(path: &Path) -> Result<String, OxenError> {
        match fs::read_to_string(path) {
            Ok(contents) => Ok(contents),
            Err(_) => {
                let err = format!("Could not open staging file {}", path.display());
                Err(OxenError::basic_str(&err))
            }
        }
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

    pub fn copy_dir_all(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> io::Result<()> {
        fs::create_dir_all(&dst)?;
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                FileUtil::copy_dir_all(entry.path(), dst.as_ref().join(entry.file_name()))?;
            } else {
                fs::copy(entry.path(), dst.as_ref().join(entry.file_name()))?;
            }
        }
        Ok(())
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
                    if FileUtil::contains_ext(&path, exts) {
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
                    if FileUtil::contains_ext(&path, exts) {
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

    pub fn path_relative_to_dir(path: &Path, dir: &Path) -> Result<PathBuf, OxenError> {
        let mut mut_path = path.to_path_buf();

        let mut components: Vec<PathBuf> = vec![];
        while mut_path.parent().is_some() {
            // println!("Comparing {:?} => {:?} => {:?}", path, mut_path, dir);
            if let Some(filename) = mut_path.file_name() {
                if mut_path != dir {
                    components.push(PathBuf::from(filename));
                } else {
                    break;
                }
            } else {
                let err = format!("Invalid filename {:?}", mut_path);
                return Err(OxenError::basic_str(&err));
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
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::util::FileUtil;

    use std::path::Path;

    #[test]
    fn file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/file.txt");
        let dir = Path::new("data/test/");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other/file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_2_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/file.txt");
        let dir = Path::new("data/test/other");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn file_path_3_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/runs/54321/file.txt");
        let dir = Path::new("data/test/runs/54321");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn full_file_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("/tmp/data/test/other/file.txt");
        let dir = Path::new("/tmp/data/test/other");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("file.txt"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other");
        let dir = Path::new("data/test/");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other"));

        Ok(())
    }

    #[test]
    fn dir_path_relative_to_another_dir() -> Result<(), OxenError> {
        let file = Path::new("data/test/other/dir");
        let dir = Path::new("data/test/");

        let relative = FileUtil::path_relative_to_dir(file, dir)?;
        assert_eq!(relative, Path::new("other/dir"));

        Ok(())
    }

    // "data/test/runs/data_14d0853f-bcdd-4f8b-9f74-82102b264968/9b0ac0fb-68e4-48c6-817a-24c2256a3efd.txt" =>
    // "data/test/runs/data_14d0853f-bcdd-4f8b-9f74-82102b264968"
}
