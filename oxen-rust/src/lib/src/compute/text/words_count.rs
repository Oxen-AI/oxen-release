//! Helper functions to compute metadata text files.
//!

use crate::error::OxenError;
use crate::util;

use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WordCountText {
    pub num_lines: usize,
}

/// Detects the text metadata for the given file.
pub fn run(path: &Path) -> Result<serde_json::Value, OxenError> {
    let num_lines = util::fs::count_lines(path)?;

    let metadata = WordCountText { num_lines };

    Ok(serde_json::to_value(metadata)?)
}

#[cfg(test)]
mod tests {
    use crate::compute::text;
    use crate::compute::text::words_count::WordCountText;
    use crate::test;

    #[test]
    fn test_get_metadata_text_readme() {
        let file = test::test_text_file_with_name("README");
        let metadata = text::words_count::run(&file).unwrap();
        let val: WordCountText = serde_json::from_value(metadata).unwrap();
        assert_eq!(val.num_lines, 3);
    }

    #[test]
    fn test_get_metadata_text_readme_md() {
        let file = test::test_text_file_with_name("README.md");
        let metadata = text::words_count::run(&file).unwrap();
        let val: WordCountText = serde_json::from_value(metadata).unwrap();
        assert_eq!(val.num_lines, 4);
    }
}
