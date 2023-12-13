//! Helper functions to get metadata from the text files.
//!

use crate::error::OxenError;
use crate::model::metadata::MetadataText;
use crate::util;

use std::path::Path;

/// Detects the text metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataText, OxenError> {
    let (line_count, char_count) = util::fs::count_lines_and_chars(path)?;
    Ok(MetadataText::new(line_count, char_count))
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::model::metadata::MetadataText;
    use crate::model::EntryDataType;
    use crate::test;

    #[test]
    fn test_get_metadata_text_readme() {
        let file = test::test_text_file_with_name("README");
        let metadata = api::local::metadata::get(file).unwrap();

        assert_eq!(metadata.size, 44);
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/plain");

        let metadata: MetadataText = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataText(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.text.num_lines, 3);
        assert_eq!(metadata.text.num_chars, 44);
    }

    #[test]
    fn test_get_metadata_text_readme_md() {
        let file = test::test_text_file_with_name("README.md");
        let metadata = api::local::metadata::get(file).unwrap();

        assert_eq!(metadata.size, 50);
        assert_eq!(metadata.data_type, EntryDataType::Text);
        assert_eq!(metadata.mime_type, "text/markdown");

        let metadata: MetadataText = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataText(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.text.num_lines, 4);
        assert_eq!(metadata.text.num_chars, 50);
    }
}
