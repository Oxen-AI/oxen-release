use serde::{Deserialize, Serialize};

use crate::model::ParsedResource;

use super::StatusMessage;

#[derive(Serialize, Deserialize, Debug)]
pub struct ParseResourceResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub resource: ParsedResource,
}
