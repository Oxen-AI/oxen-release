use serde::{Deserialize, Serialize};

use crate::model::MetaDataEntry;

use super::StatusMessage;

#[derive(Deserialize, Serialize, Debug)]
pub struct EntryMetaDataResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub entry: MetaDataEntry,
}
