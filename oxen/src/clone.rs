use pyo3::prelude::*;

use liboxen::command;
use liboxen::opts::CloneOpts;

use std::path::PathBuf;

#[pyfunction]
#[pyo3(signature = (url, dst, branch=String::from("main"), shallow=false))]
pub fn clone(url: String, dst: String, branch: String, shallow: bool) -> PyResult<()> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        async_clone(url, dst, branch, shallow).await;
    });
   Ok(())
}

async fn async_clone(url: String, dst: String, branch: String, shallow: bool) {
    let opts = CloneOpts {
        url: url.to_string(),
        dst: PathBuf::from(dst),
        branch: branch.to_string(),
        shallow,
    };
    log::info!("Cloning... {url}");
    match command::clone(&opts).await {
        Ok(_) => {
            log::info!("Success!");
        },
        Err(err) => {
            log::error!("Error: {}", err);
        }
    }
}
