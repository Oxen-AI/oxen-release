use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct NameParam {
    pub name: Option<String>,
}
