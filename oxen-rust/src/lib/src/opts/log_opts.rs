#[derive(Clone, Debug)]
pub struct LogOpts {
    pub committish: Option<String>, // commit id or branch name
    pub remote: bool,
}
