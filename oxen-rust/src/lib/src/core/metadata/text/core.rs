//! Helper functions to compute metadata text files.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::MetadataText;
use crate::util;

use std::path::Path;

/// Detects the text metadata for the given file.
pub fn run(path: &Path) -> Result<serde_json::Value, OxenError> {
    let num_lines = util::fs::count_lines(path)?;
    let metadata = MetadataText { num_lines };

    Ok(serde_json::to_value(metadata)?)
}

#[cfg(test)]
mod tests {
    use crate::core::metadata::text;
    use crate::model::entry::metadata_entry::MetadataText;
    use crate::test;

    #[test]
    fn test_compute_text_line_count_text_readme() {
        let file = test::test_text_file_with_name("README");
        let metadata = text::core::run(&file).unwrap();
        let val: MetadataText = serde_json::from_value(metadata).unwrap();
        assert_eq!(val.num_lines, 3);
    }

    #[test]
    fn test_compute_text_line_count_text_readme_md() {
        let file = test::test_text_file_with_name("README.md");
        let metadata = text::core::run(&file).unwrap();
        let val: MetadataText = serde_json::from_value(metadata).unwrap();
        assert_eq!(val.num_lines, 4);
    }
}
