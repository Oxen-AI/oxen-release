#[derive(Clone, Debug)]
pub struct LogOpts {
    pub revision: Option<String>, // commit id or branch name
    pub remote: bool,
}
