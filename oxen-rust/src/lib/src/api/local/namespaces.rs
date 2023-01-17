use bytesize;
use fs_extra::dir::get_size;
use std::path::Path;

use crate::error::OxenError;
use crate::model::Namespace;
use crate::util;

pub fn list(path: &Path) -> Vec<String> {
    log::debug!("api::local::namespaces::list",);
    let mut results: Vec<String> = vec![];

    if let Ok(dir) = std::fs::read_dir(path) {
        for entry in dir.into_iter().filter_map(|e| e.ok()) {
            // if the directory has a .oxen dir, let's add it, otherwise ignore
            let path = entry.path();

            log::debug!("api::local::namespaces::list checking path {:?}", path);

            if path.is_dir() && !util::fs::is_in_oxen_hidden_dir(&path) {
                results.push(path.file_name().unwrap().to_str().unwrap().to_string())
            }
        }
    }

    results
}

// return the named namespace object
pub fn get(data_dir: &Path, name: &str) -> Result<Option<Namespace>, OxenError> {
    log::debug!("api::local::namespaces::get {}", name);
    let path = data_dir.join(name);

    if !path.is_dir() {
        return Ok(None);
    }

    let mut namespace = Namespace {
        name: name.to_string(),
        storage_usage_gb: 0.0,
    };

    match get_size(path) {
        Ok(size) => {
            namespace.storage_usage_gb = size as f64 / bytesize::GB as f64;
        }
        Err(e) => {
            log::error!("api::local::namespaces::get error getting size: {}", e);
            return Err(OxenError::basic_str(e.to_string()));
        }
    }

    Ok(Some(namespace))
}
