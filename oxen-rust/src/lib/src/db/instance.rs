// use crate::error::OxenError;

// use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};

// use std::path::{Path, PathBuf};
// use std::collections::HashMap;
// use std::sync::Mutex;

// lazy_static! {
//     static ref DATABASES: Mutex<HashMap<String, &'static DBWithThreadMode<MultiThreaded>>> = {
//         Mutex::new(HashMap::new())
//     };

//     static ref NAMES_TO_PATHS: Mutex<HashMap<String, PathBuf>> = {
//         Mutex::new(HashMap::new())
//     };
// }

// pub fn create(name: String, path: &Path) -> Result<(), OxenError> {
//     let mut opts = Options::default();
//     opts.set_log_level(LogLevel::Error);
//     opts.create_if_missing(true);

//     let db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open(&opts, &path)?;

//     let mut locked_dbs = DATABASES.lock().unwrap();
//     locked_dbs.insert(name.clone(), &db);

//     Ok(())
// }

// pub fn get(name: String) -> Option<&'static &'static DBWithThreadMode<MultiThreaded>> {
//     let mut locked_dbs = DATABASES.lock().unwrap();
//     locked_dbs.get(&name)
// }

// pub fn insert<T: AsRef<str>>(name: T, key: T, value: T) -> Result<(), OxenError> {
//     let mut locked_dbs = DATABASES.lock().unwrap();

//     Ok(())
// }
