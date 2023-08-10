use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataText {
    pub text: MetadataTextImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct MetadataTextImpl {
    pub num_lines: usize,
    pub num_chars: usize,
}

impl MetadataText {
    pub fn new(num_lines: usize, num_chars: usize) -> Self {
        Self {
            text: MetadataTextImpl {
                num_lines,
                num_chars,
            },
        }
    }
}
