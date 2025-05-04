#[derive(Clone, Debug)]
pub struct NotebookOpts {
    pub notebook: String, // path or id
    pub branch: String,
    pub base_image: String,
    pub mode: String, // "edit", "script"
    pub script_args: Option<String>,
}
