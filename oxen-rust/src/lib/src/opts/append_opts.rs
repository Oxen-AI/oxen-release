use crate::model::ContentType;

#[derive(Clone, Debug)]
pub struct AppendOpts {
    pub content_type: ContentType,
    pub remote: bool,
}
