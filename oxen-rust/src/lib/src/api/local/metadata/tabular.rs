//! Helper functions to get metadata from tabular files.
//!

use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::entry::metadata_entry::MetadataTabular;
use crate::opts::DFOpts;

use std::path::Path;

/// Detects the audio metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataTabular, OxenError> {
    let path = path.as_ref();
    let opts = DFOpts::empty();
    let df = tabular::read_df(path, opts)?;
    Ok(MetadataTabular {
        height: df.height(),
        width: df.width(),
    })
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::EntryDataType;
    use crate::test;

    #[test]
    fn test_get_metadata_tabular() {
        let file = test::test_text_file_with_name("celeb_a_200k.csv");
        let metadata = api::local::metadata::get(file).unwrap();

        assert_eq!(metadata.size, 9604701);
        assert_eq!(metadata.data_type, EntryDataType::Tabular);
        assert_eq!(metadata.mime_type, "text/plain");
        // assert!(metadata.meta.tabular.is_some());
        // let meta = metadata.meta.tabular.unwrap();
        // assert_eq!(meta.width, 11);
        // assert_eq!(meta.height, 200_000);
    }
}
