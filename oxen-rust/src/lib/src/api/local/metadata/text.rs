//! Helper functions to get metadata from the text files.
//!

use crate::error::OxenError;
use crate::model::metadata::MetadataText;

use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;

/// Detects the text metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataText, OxenError> {
    let path = path.as_ref();
    let file = File::open(path)?;

    let metadata = p_compute_metadata(file)?;
    Ok(metadata)
}

fn p_compute_metadata<R: std::io::Read>(handle: R) -> Result<MetadataText, std::io::Error> {
    let mut reader = BufReader::with_capacity(1024 * 32, handle);
    let mut line_count = 1;
    let mut char_count = 0;
    loop {
        let len = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            line_count += bytecount::count(buf, b'\n');
            char_count += bytecount::num_chars(buf);
            buf.len()
        };
        reader.consume(len);
    }
    Ok(MetadataText {
        num_lines: line_count,
        num_chars: char_count,
    })
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

        assert_eq!(metadata.num_lines, 3);
        assert_eq!(metadata.num_chars, 44);
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

        assert_eq!(metadata.num_lines, 4);
        assert_eq!(metadata.num_chars, 50);
    }
}
