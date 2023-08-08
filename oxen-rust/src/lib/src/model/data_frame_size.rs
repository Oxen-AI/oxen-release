use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DataFrameSize {
    pub height: usize,
    pub width: usize,
}

impl DataFrameSize {
    pub fn is_empty(&self) -> bool {
        self.height == 0 && self.width == 0
    }
}
