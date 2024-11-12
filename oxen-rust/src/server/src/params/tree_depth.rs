use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, Debug)]
pub struct TreeDepthQuery {
    pub depth: Option<i32>,
    pub subtree: Option<PathBuf>,
}
