use crate::error::OxenError;
use crate::util::FileUtil;

use rocksdb::{IteratorMode, DB};
use std::collections::HashSet;
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::str;

pub struct Stager {
    db: DB,
    repo_path: PathBuf,
}

impl Stager {
    pub fn new(dbpath: &Path, repo_path: &Path) -> Result<Stager, OxenError> {
        Ok(Stager {
            db: DB::open_default(dbpath)?,
            repo_path: repo_path.to_path_buf(),
        })
    }

    pub fn add(&self, path: &Path) -> Result<(), OxenError> {
        if path.is_dir() {
            match self.add_dir(path) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            match self.add_file(path) {
                Ok(_) => Ok(()),
                Err(err) => Err(err),
            }
        }
    }

    fn list_image_files_from_dir(&self, dir: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["jpg", "jpeg", "png"]
            .into_iter()
            .map(String::from)
            .collect();
        FileUtil::recursive_files_with_extensions(&dir, &img_ext)
            .into_iter()
            .map(|file| FileUtil::path_relative_to_dir(&file, &self.repo_path).unwrap() )
            .filter(|file| !self.file_is_in_index(file) )
            .collect()
    }

    fn list_text_files_from_dir(&self, dir: &Path) -> Vec<PathBuf> {
        let img_ext: HashSet<String> = vec!["txt"].into_iter().map(String::from).collect();
        FileUtil::recursive_files_with_extensions(&dir, &img_ext)
            .into_iter()
            .map(|file| FileUtil::path_relative_to_dir(&file, &self.repo_path).unwrap() )
            .filter(|file| !self.file_is_in_index(file) )
            .collect()
    }

    fn count_untracked_files_in_dir(&self, dir: &Path) -> usize {
        let files = self.list_untracked_files_in_dir(&dir);
        files.len()
    }

    fn list_untracked_files_in_dir(&self, path: &Path) -> Vec<PathBuf> {
        let mut paths: Vec<PathBuf> = vec![];
        let mut img_paths = self.list_image_files_from_dir(&path);
        let mut txt_paths = self.list_text_files_from_dir(&path);

        // println!("Found {} images", img_paths.len());
        // println!("Found {} text files", txt_paths.len());

        paths.append(&mut img_paths);
        paths.append(&mut txt_paths);
        paths
    }

    pub fn add_dir(&self, path: &Path) -> Result<usize, OxenError> {
        if !path.exists() {
            let err = format!("Stager.add_dir({:?}) cannot stage non-existant dir", path);
            return Err(OxenError::basic_str(&err));
        }

        let relative_path = FileUtil::path_relative_to_dir(&path, &self.repo_path)?;
        let key = relative_path.to_str().unwrap().as_bytes();

        // Add all files, and get a count
        let paths: Vec<PathBuf> = self.list_untracked_files_in_dir(&path);

        // TODO: Find dirs and recursively add
        let count: usize = paths.len();

        self.add_dir_count(&key, count)
    }

    fn add_dir_count(&self, key: &[u8], count: usize) -> Result<usize, OxenError> {
        // store count in little endian
        match self.db.put(key, count.to_le_bytes()) {
            Ok(_) => {
                Ok(count)
            },
            Err(err) => {
                let err = format!("Error adding key {}", err);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn add_file(&self, path: &Path) -> Result<PathBuf, OxenError> {
        // We should have normalized to path past repo at this point
        let full_path = self.repo_path.join(path);
        if !path.exists() && !full_path.exists() {
            let err = format!("Stage.add_file({:?}) cannot stage non-existant file", path);
            return Err(OxenError::basic_str(&err));
        }

        // Key is the filename relative to the repository
        // if repository: /Users/username/Datasets/MyRepo
        //   /Users/username/Datasets/MyRepo/train -> train
        //   /Users/username/Datasets/MyRepo/annotations/train.txt -> annotations/train.txt
        let path = FileUtil::path_relative_to_dir(&path, &self.repo_path)?;
        let key = path.to_str().unwrap().as_bytes();

        // println!("Adding key {}", path.to_str().unwrap());
        // Value is initially empty, meaning we still have to hash, but just keeping track of what is staged
        // Then when we push, we hash the file contents and save it back in here to keep track of progress
        self.db.put(&key, b"")?;

        // Check if we have added the full directory, 
        // if we have, remove all the individual keys
        // and add the full directory
        // println!("Checking parent of file: {:?}", path);
        if let Some(parent) = path.parent() {
            // println!("Parent {:?} is_dir {}", parent, parent.is_dir());
            if parent != Path::new("") {
                let full_path = self.repo_path.join(parent);
                // println!("Getting count for parent {:?} full path: {:?}", parent, full_path);
                let untracked_files = self.list_untracked_files_in_dir(&full_path);
                // println!("Got {} untracked files", untracked_files.len());
                if untracked_files.is_empty() {
                    let to_remove = self.list_keys_with_prefix(parent.to_str().unwrap())?;
                    let count = to_remove.len();
                    // println!("Remove {} keys", to_remove.len());
                    for key in to_remove.iter() {
                        match self.db.delete(key) {
                            Ok(_) => {
                                // println!("Deleted key: {}", key);
                            },
                            Err(err) => {
                                eprintln!("Unable to delete key [{}] err: {}", key, err);
                            }
                        }
                    }

                    let key = parent.to_str().unwrap().as_bytes();
                    self.add_dir_count(&key, count)?;
                }
            }
        }

        Ok(path)
    }

    fn list_keys_with_prefix(&self, path: &str) -> Result<Vec<String>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut keys: Vec<String> = vec![];
        for (key, _) in iter {
            let key = String::from(str::from_utf8(&*key)?);
            if key.starts_with(path) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    pub fn list_added_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<PathBuf> = vec![];
        for (key, _) in iter {
            let local_path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            let full_path = self.repo_path.join(&local_path);
            if full_path.is_file() {
                paths.push(local_path);
            }
        }
        Ok(paths)
    }

    pub fn list_added_directories(&self) -> Result<Vec<(PathBuf, usize)>, OxenError> {
        let iter = self.db.iterator(IteratorMode::Start);
        let mut paths: Vec<(PathBuf, usize)> = vec![];
        for (key, value) in iter {
            let local_path = PathBuf::from(String::from(str::from_utf8(&*key)?));
            let full_path = self.repo_path.join(&local_path);
            if full_path.is_dir() {
                match self.convert_usize_slice(&*value) {
                    Ok(size) => {
                        paths.push((local_path, size));
                    }
                    Err(err) => {
                        eprintln!(
                            "Could not convert data attached to: {:?}\nErr:{}",
                            full_path, err
                        )
                    }
                }
            }
        }
        Ok(paths)
    }

    pub fn list_untracked_files(&self) -> Result<Vec<PathBuf>, OxenError> {
        // We just look at the top level here for summary..not recursively right now

        let dir_entries = std::fs::read_dir(&self.repo_path)?;
        // println!("Listing untracked files from {:?}", dir_entries);

        let mut paths: Vec<PathBuf> = vec![];
        for entry in dir_entries {
            let local_path = entry?.path();
            if local_path.is_file() {
                // Return relative path with respect to the repo
                let relative_path = FileUtil::path_relative_to_dir(&local_path, &self.repo_path)?;
                // println!("Checking if we have the key? {:?}", relative_path);
                if let Some(path_str) = relative_path.to_str() {
                    let bytes = path_str.as_bytes();
                    match self.db.get(bytes) {
                        Ok(Some(_value)) => {
                            // already added
                            // println!("got value: {:?}", value);
                        }
                        Ok(None) => {
                            // did not get val
                            // println!("untracked! {:?}", relative_path);
                            paths.push(relative_path);
                        }
                        Err(err) => {
                            eprintln!("{}", err);
                        }
                    }
                }
            }
        }

        Ok(paths)
    }

    fn file_is_in_index(&self, path: &Path) -> bool {
        if let Some(path_str) = path.to_str() {
            let bytes = path_str.as_bytes();
            match self.db.get(bytes) {
                Ok(Some(_value)) => {
                    // already added
                    true
                }
                Ok(None) => {
                    // did not get val
                    false
                }
                Err(err) => {
                    eprintln!("could not fetch value from db: {}", err);
                    false
                }
            }
        } else {
            eprintln!("could not convert path to str: {:?}", path);
            false
        }
    }

    pub fn list_untracked_directories(&self) -> Result<Vec<(PathBuf, usize)>, OxenError> {
        // println!("list_untracked_directories {:?}", self.repo_path);
        let dir_entries = std::fs::read_dir(&self.repo_path)?;

        let mut paths: Vec<(PathBuf, usize)> = vec![];
        for entry in dir_entries {
            let path = entry?.path();
            // println!("list_untracked_directories considering {:?}", path);
            if path.is_dir() {
                let relative_path = FileUtil::path_relative_to_dir(&path, &self.repo_path)?;
                // println!("list_untracked_directories relative {:?}", relative_path);

                if let Some(path_str) = relative_path.to_str() {
                    if path_str.contains(".oxen") {
                        continue;
                    }

                    let bytes = path_str.as_bytes();
                    match self.db.get(bytes) {
                        Ok(Some(_value)) => {
                            // already added
                            // println!("got value: {:?}", value);
                        }
                        Ok(None) => {
                            // did not get val
                            // println!("list_untracked_directories get file count in: {:?}", path);
                            let count = self.count_untracked_files_in_dir(&path);
                            paths.push((relative_path, count));
                        }
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
            }
            Err(err) => {
                let err = format!("Unable to convert data to usize: {:?}\nErr: {}", slice, err);
                Err(OxenError::basic_str(&err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::error::OxenError;
    use crate::test;
    use crate::util::FileUtil;

    use std::path::{PathBuf, Path};

    const BASE_DIR: &str = "data/test/runs";

    #[test]
    fn test_1_add_file() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;
        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello World")?;

        match stager.add_file(&hello_file) {
            Ok(path) => {
                let relative_path = FileUtil::path_relative_to_dir(&hello_file, &repo_path)?;
                assert_eq!(path, relative_path);
            }
            Err(err) => {
                panic!("test_add_file() Should have returned path... {}", err)
            }
        }

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_add_twice_only_adds_once() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Make sure we have a valid file
        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello World")?;

        // Add it twice
        stager.add_file(&hello_file)?;
        stager.add_file(&hello_file)?;

        let files = stager.list_added_files()?;
        assert_eq!(files.len(), 1);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_add_non_existant_file() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        let hello_file = PathBuf::from("non-existant.txt");
        if stager.add_file(&hello_file).is_ok() {
            // we don't want to be able to add this file
            panic!("test_add_non_existant_file() Cannot stage non-existant file")
        }

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_add_directory() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to directories
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

        match stager.add_dir(&sub_dir) {
            Ok(num_files) => {
                assert_eq!(2, num_files);
            }
            Err(err) => {
                panic!("test_add_directory() Should have returned path... {}", err)
            }
        }

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_files() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;
        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello World")?;

        // Stage file
        stager.add_file(&hello_file)?;

        // List files
        let files = stager.list_added_files()?;
        assert_eq!(files.len(), 1);
        let relative_path = FileUtil::path_relative_to_dir(&hello_file, &repo_path)?;
        assert_eq!(files[0], relative_path);

        // cleanup
        std::fs::remove_dir_all(repo_path)?;
        std::fs::remove_dir_all(db_path)?;

        Ok(())
    }

    #[test]
    fn test_add_file_in_sub_dir() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to a sub directory
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

        stager.add_file(&sub_file)?;

        // List files
        let files = stager.list_added_files()?;

        // There is one file
        assert_eq!(files.len(), 1);
        let relative_path = FileUtil::path_relative_to_dir(&sub_file, &repo_path)?;
        assert_eq!(files[0], relative_path);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_add_file_in_sub_dir_updates_untracked_count() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to a sub directory
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
        let sub_file = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

        let dirs = stager.list_untracked_directories()?;
        // There is one directory
        assert_eq!(dirs.len(), 1);
        let relative_path = FileUtil::path_relative_to_dir(&sub_dir, &repo_path)?;
        assert_eq!(dirs[0].0, relative_path);

        // With three untracked files
        assert_eq!(dirs[0].1, 3);

        // Then we add one file
        stager.add_file(&sub_file)?;

        // There are still two untracked files in the dir
        let dirs = stager.list_untracked_directories()?;
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].0, relative_path);

        // With two files
        assert_eq!(dirs[0].1, 2);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_add_all_files_in_sub_dir() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to a sub directory
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        let sub_file_1 = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let sub_file_2 = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;
        let sub_file_3 = test::add_txt_file_to_dir(&sub_dir, "Hello 3")?;

        let dirs = stager.list_untracked_directories()?;
        
        // There is one directory
        assert_eq!(dirs.len(), 1);
        // With three untracked files
        assert_eq!(dirs[0].1, 3);

        // Then we add all three
        stager.add_file(&sub_file_1)?;
        stager.add_file(&sub_file_2)?;
        stager.add_file(&sub_file_3)?;

        // There now there are no untracked directories
        let untracked_dirs = stager.list_untracked_directories()?;
        assert_eq!(untracked_dirs.len(), 0);

        // And there is one tracked directory
        let added_dirs = stager.list_added_directories()?;
        assert_eq!(added_dirs.len(), 1);
        assert_eq!(added_dirs[0].1, 3);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_directories() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to a sub directory
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

        stager.add_dir(&sub_dir)?;

        // List files
        let dirs = stager.list_added_directories()?;

        // There is one directory
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].0, Path::new("training_data"));

        // With two files
        assert_eq!(dirs[0].1, 2);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_untracked_files() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;
        let hello_file = test::add_txt_file_to_dir(&repo_path, "Hello 1")?;

        // Do not add...

        // List files
        let files = stager.list_untracked_files()?;
        assert_eq!(files.len(), 1);
        let relative_path = FileUtil::path_relative_to_dir(&hello_file, &repo_path)?;
        assert_eq!(files[0], relative_path);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_untracked_dirs() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        // Do not add...

        // List files
        let files = stager.list_untracked_directories()?;
        assert_eq!(files.len(), 1);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_one_untracked_directory() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Write two files to a sub directory
        let sub_dir = repo_path.join("training_data");
        std::fs::create_dir_all(&sub_dir)?;

        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 1")?;
        let _ = test::add_txt_file_to_dir(&sub_dir, "Hello 2")?;

        // Do not add...

        // List files
        let files = stager.list_untracked_directories()?;

        // There is one directory
        assert_eq!(files.len(), 1);

        // With two files
        assert_eq!(files[0].1, 2);

        // cleanup
        std::fs::remove_dir_all(db_path)?;
        std::fs::remove_dir_all(repo_path)?;

        Ok(())
    }

    #[test]
    fn test_list_untracked_directories_after_add() -> Result<(), OxenError> {
        let (stager, repo_path, db_path) = test::create_stager(BASE_DIR)?;

        // Create 2 sub directories, one with  Write two files to a sub directory
        let train_dir = repo_path.join("train");
        std::fs::create_dir_all(&train_dir)?;
        let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_1.jpg"))?;
        let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_1.jpg"))?;
        let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/cat_2.jpg"))?;
        let _ = test::add_img_file_to_dir(&train_dir, Path::new("data/test/images/dog_2.jpg"))?;

        let test_dir = repo_path.join("test");
        std::fs::create_dir_all(&test_dir)?;
        let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/cat_3.jpg"))?;
        let _ = test::add_img_file_to_dir(&test_dir, Path::new("data/test/images/dog_3.jpg"))?;

        let valid_dir = repo_path.join("valid");
        std::fs::create_dir_all(&valid_dir)?;
        let _ = test::add_img_file_to_dir(&valid_dir, Path::new("data/test/images/dog_4.jpg"))?;

        let base_file_1 = test::add_txt_file_to_dir(&repo_path, "Hello 1")?;
        let _base_file_2 = test::add_txt_file_to_dir(&repo_path, "Hello 2")?;
        let _base_file_3 = test::add_txt_file_to_dir(&repo_path, "Hello 3")?;

        // At first there should be 3 untracked
        let untracked_dirs = stager.list_untracked_directories()?;
        assert_eq!(untracked_dirs.len(), 3);

        // Add the directory
        let _ = stager.add_dir(&train_dir)?;
        // Add one file
        let _ = stager.add_file(&base_file_1)?;

        // List the files
        let added_files = stager.list_added_files()?;
        let added_dirs = stager.list_added_directories()?;
        // for file in added_files.iter() {
        //     println!("ADDED FILE {:?}", file);
        // }
        // for dir in added_dirs.iter() {
        //     println!("ADDED DIR {:?}", dir);
        // }
        // println!("---");
        let untracked_files = stager.list_untracked_files()?;
        let untracked_dirs = stager.list_untracked_directories()?;
        // for file in untracked_files.iter() {
        //     println!("UNTRACKED FILE {:?}", file);
        // }
        // for dir in untracked_dirs.iter() {
        //     println!("UNTRACKED DIR {:?}", dir);
        // }

        // There is 1 added file and 1 added dir
        assert_eq!(added_files.len(), 1);
        assert_eq!(added_dirs.len(), 1);

        // There are 2 untracked files at the top level
        assert_eq!(untracked_files.len(), 2);
        // There are 2 untracked dirs at the top level
        assert_eq!(untracked_dirs.len(), 2);

        // cleanup
        std::fs::remove_dir_all(repo_path)?;
        std::fs::remove_dir_all(db_path)?;

        Ok(())
    }
}
