use std::path::Path;

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
