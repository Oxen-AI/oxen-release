use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct PageNumQuery {
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}
