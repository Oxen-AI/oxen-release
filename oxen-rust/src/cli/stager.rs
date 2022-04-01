use crate::error::OxenError;
use crate::util::FileUtil;

use std::path::{Path, PathBuf};
use rocksdb::{DB, IteratorMode};
use std::str;
use std::collections::{HashSet};
use std::convert::TryFrom;

pub struct Stager {
    db: DB,
    repo_path: PathBuf
}

impl Stager {
    pub fn new(dbpath: &Path, repo_path: &PathBuf) -> Result<Stager, OxenError> {
        Ok(Stager {
            db: DB::open_default(dbpath)?,
            repo_path: repo_path.clone(),
        })
    }

    pub fn add(&self, path: &Path) -> Result<(), OxenError> {
        if path.is_dir() {
            match self.add_dir(path) {
                Ok(_) => Ok(()),
                Err(err) => Err(err)
            }
        } else {
            match self.add_file(path) {
                Ok(_) => Ok(()),
                Err(err) => Err(err)
            }
        }
    }

    fn list_image_files_from_dir(&self, dirname: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["jpg", "jpeg", "png"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dirname, &img_ext)
    }

    fn list_text_files_from_dir(&self, dirname: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["txt"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(dirname, &img_ext)
    }

    pub fn add_dir(&self, path: &Path) -> Result<usize, OxenError> {
        if let Some(file_str) = path.to_str() {
            if !path.exists() {
                let err = format!("Cannot stage non-existant file: {:?}", path);
                return Err(OxenError::basic_str(&err));
            }

            println!("Staging dir {:?}", path);
            // Key is the directory name
            let key = file_str.as_bytes();

            // Add all files, and get a count
            let mut paths: Vec<PathBuf> = vec![];
            let mut img_paths = self.list_image_files_from_dir(path);
            let mut txt_paths = self.list_text_files_from_dir(path);

            println!("Found {} images", img_paths.len());
            println!("Found {} text files", txt_paths.len());

            paths.append(&mut img_paths);
            paths.append(&mut txt_paths);

            for path in paths.iter() {
                self.add_file(&path)?;
            }

            // TODO: Find dirs and recursively add
            // store count in little endian
            let count: usize = paths.len();
            println!("Staged {} files", count);
            self.db.put(key, count.to_le_bytes()).unwrap();

            Ok(count)
        } else {
            let err = format!("Could not stage file {:?}", &path);
            Err(OxenError::basic_str(&err))
        }
    }

    pub fn add_file(&self, path: &Path) -> Result<String, OxenError> {
        let full_path = path.canonicalize()?;
        if !path.exists() {
            let err = format!("Cannot stage non-existant file: {:?}", path);
            return Err(OxenError::basic_str(&err));
        }

        // Key is the filename
        if let Some(file_str) = full_path.to_str() {
            let key = file_str.as_bytes();

            // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
            // Then when we push, we hash the file contents and save it back in here to keep track of progress
            self.db.put(key, b"")?;

            Ok(String::from(file_str))
        } else {
            let err = format!("Could not convert file path to string {:?}", &path);
            Err(OxenError::basic_str(&err))
        }
    }

    pub fn list_added_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<PathBuf> = vec![];
        for (key, _) in iter {
            let path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            if path.is_file() {
                paths.push(path);
            }
        }
        Ok(paths)
    }

    pub fn list_added_directories(&self) -> Result<Vec<(PathBuf, usize)>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<(PathBuf, usize)> = vec![];
        for (key, value) in iter {
            let path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            if path.is_dir() {
                match self.convert_usize_slice(&*value) {
                    Ok(size) => {
                        paths.push((path, size));
                    },
                    Err(err) => {
                        eprintln!("Could not convert data attached to: {:?}\nErr:{}", path, err)
                    }
                }
            }
        }
        Ok(paths)
    }

    pub fn list_untracked_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let dir_entries = std::fs::read_dir(&self.repo_path)?;

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let path = entry?.path();
            if path.is_file() {
                let path = path.canonicalize()?;
                // println!("checking path: {:?}", path);
                if let Some(path_str) = path.to_str() {
                    let bytes = path_str.as_bytes();
                    match self.db.get(bytes) {
                        Ok(Some(_value)) => {
                            // already added
                            // println!("got value: {:?}", value);
                        },
                        Ok(None) => {
                            // did not get val
                            // println!("untracked! {:?}", path);
                            paths.push(path);
                        },
                        Err(err) => {
                            eprintln!("{}", err);
                        }
                    }
                } 
            }
        }

        Ok(paths)
    }

    fn convert_usize_slice(&self, slice: &[u8]) -> Result<usize, OxenError> {
        match <[u8; 8]>::try_from(slice) {
            Ok(data) => {
                let size: usize = usize::from_le_bytes(data);
                Ok(size)
            },
            Err(err) => {
                let err = format!("Unable to convert data to usize: {:?}\nErr: {}", slice, err);
                Err(OxenError::basic_str(&err))
            }
            
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::stager::Stager;
    use crate::error::OxenError;
    use std::path::{PathBuf, Path};
    use std::fs::File;
    use std::io::prelude::*;

    #[test]
    fn test_add_file() -> Result<(), OxenError> {
        let dbname = uuid::Uuid::new_v4();
        let db_dir = format!("/tmp/{}", dbname);
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        // Make sure we have a valid file
        let hello_file = PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4()));
        let mut file = File::create(&hello_file)?;
        file.write_all(b"Hello, world!")?;

        match stager.add_file(&hello_file) {
            Ok(path) => {
                if let Some(full_path) = hello_file.canonicalize()?.to_str() {
                    assert_eq!(path, full_path);
                } else {
                    panic!("test_add_file() Did not return full path")
                }
            },
            Err(err) => {
                panic!("test_add_file() Should have returned path... {}", err)
            }
        }

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_file(hello_file)?;

        Ok(())
    }

    #[test]
    fn test_add_file_twice_only_adds_once() -> Result<(), OxenError> {
        let dbname = uuid::Uuid::new_v4();
        let db_dir = format!("/tmp/{}", dbname);
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        // Make sure we have a valid file
        let hello_file = PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4()));
        let mut file = File::create(&hello_file)?;
        file.write_all(b"Hello, world!")?;

        stager.add_file(&hello_file)?;
        stager.add_file(&hello_file)?;

        let files = stager.list_added_files()?;
        assert_eq!(files.len(), 1);

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_file(hello_file)?;

        Ok(())
    }

    #[test]
    fn test_add_non_existant_file() -> Result<(), OxenError> {
        let dbname = uuid::Uuid::new_v4();
        let db_dir = format!("/tmp/{}", dbname);
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        let hello_file = PathBuf::from("non-existant.txt");
        match stager.add_file(&hello_file) {
            Ok(_) => {
                panic!("test_add_non_existant_file() Cannot stage non-existant file")
            },
            Err(_) => {
                // we want an error
            }
        }

        // cleanup
        std::fs::remove_dir_all(stager_path)?;

        Ok(())
    }

    #[test]
    fn test_add_directory() -> Result<(), OxenError> {
        let db_dir = format!("/tmp/{}", uuid::Uuid::new_v4());
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        // Make sure we have a valid directory with files
        let data_dirname = format!("/tmp/{}", uuid::Uuid::new_v4());
        let data_dirpath = Path::new(&data_dirname);
        std::fs::create_dir(&data_dirpath)?;

        // Write two files to directories
        let file_1 = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&file_1)?;
        file.write_all(b"Hello 1")?;

        let file_2 = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&file_2)?;
        file.write_all(b"Hello 2")?;

        match stager.add_dir(&data_dirpath) {
            Ok(num_files) => {
                assert_eq!(2, num_files);
            },
            Err(err) => {
                panic!("test_add_directory() Should have returned path... {}", err)
            }
        }

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_dir_all(data_dirpath)?;

        Ok(())
    }

    #[test]
    fn test_list_files() -> Result<(), OxenError> {
        let dbname = uuid::Uuid::new_v4();
        let db_dir = format!("/tmp/{}", dbname);
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        let hello_file = PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4()));
        let mut file = File::create(&hello_file)?;
        file.write_all(b"Hello, world!")?;

        // Stage file
        stager.add_file(&hello_file)?;

        // List files
        let files = stager.list_added_files()?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], hello_file.canonicalize()?);

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_file(hello_file)?;

        Ok(())
    }

    #[test]
    fn test_list_directories() -> Result<(), OxenError> {
        let db_dir = format!("/tmp/{}", uuid::Uuid::new_v4());
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;
        
        // Make sure we have a valid directory with files
        let data_dirname = format!("/tmp/{}", uuid::Uuid::new_v4());
        let data_dirpath = Path::new(&data_dirname);
        std::fs::create_dir(&data_dirpath)?;

        // Write two files to directories
        let file_1 = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&file_1)?;
        file.write_all(b"Hello 1")?;

        let file_2 = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&file_2)?;
        file.write_all(b"Hello 2")?;

        stager.add_dir(&data_dirpath)?;

        // List files
        let files = stager.list_added_directories()?;
        
        // There is one directory
        assert_eq!(files.len(), 1);

        // With two files
        assert_eq!(files[0].1, 2);

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_dir_all(data_dirpath)?;

        Ok(())
    }

    #[test]
    fn test_list_untracked_files() -> Result<(), OxenError> {
        let dbname = uuid::Uuid::new_v4();
        let db_dir = format!("/tmp/{}", dbname);
        let db_path = Path::new(&db_dir);
        let stager_path = PathBuf::from("./");
        let stager = Stager::new(&db_path, &stager_path)?;

        let data_dirname = format!("/tmp/{}", uuid::Uuid::new_v4());
        let data_dirpath = Path::new(&data_dirname);
        let hello_file = data_dirpath.join(PathBuf::from(format!("{}.txt", uuid::Uuid::new_v4())));
        let mut file = File::create(&hello_file)?;
        file.write_all(b"Hello, world!")?;

        // Do not add...

        // List files
        let files = stager.list_untracked_files()?;
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], hello_file.canonicalize()?);

        // cleanup
        std::fs::remove_dir_all(stager_path)?;
        std::fs::remove_dir_all(data_dirpath)?;
        std::fs::remove_file(hello_file)?;

        Ok(())
    }
}