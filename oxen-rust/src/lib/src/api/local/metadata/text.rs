//! Helper functions to get metadata from the text files.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::MetadataText;
use crate::util;

use std::path::Path;

/// Detects the text metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataText, OxenError> {
    let path = path.as_ref();
    let num_lines = util::fs::count_lines(path)?;
    Ok(MetadataText { num_lines })
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::EntryDataType;
    use crate::test;

    #[test]
    fn test_get_metadata_text_readme() {
        let file = test::test_text_file_with_name("README");
        let metadata = api::local::metadata::get(file).unwrap();

        assert_eq!(metadata.size, 44);
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/plain");
        // assert!(metadata.meta.text.is_some());
        // assert_eq!(metadata.meta.text.unwrap().num_lines, 3);
    }

    #[test]
    fn test_get_metadata_text_readme_md() {
        let file = test::test_text_file_with_name("README.md");
        let metadata = api::local::metadata::get(file).unwrap();

        assert_eq!(metadata.size, 50);
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/markdown");
        // assert!(metadata.meta.text.is_some());
        // assert_eq!(metadata.meta.text.unwrap().num_lines, 4);
    }
}
