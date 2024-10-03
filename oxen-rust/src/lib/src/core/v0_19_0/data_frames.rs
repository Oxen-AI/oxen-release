use crate::core::df::tabular;
use crate::error::OxenError;
use crate::model::data_frame::{DataFrameSchemaSize, DataFrameSlice, DataFrameSliceSchemas};
use crate::model::metadata::generic_metadata::GenericMetadata;
use crate::model::metadata::metadata_tabular::MetadataTabularImpl;
use crate::model::{Commit, DataFrameSize, LocalRepository, Schema};
use crate::opts::DFOpts;
use crate::{repositories, util};

use std::path::Path;

pub mod schemas;

pub fn get_slice(
    repo: &LocalRepository,
    commit: &Commit,
    path: impl AsRef<Path>,
    opts: &DFOpts,
) -> Result<DataFrameSlice, OxenError> {
    // Get the file node
    let file_node = repositories::tree::get_file_by_path(repo, commit, &path)?
        .ok_or(OxenError::path_does_not_exist(path.as_ref()))?;

    let metadata: Result<MetadataTabularImpl, OxenError> = match file_node.metadata {
        Some(metadata) => match metadata {
            GenericMetadata::MetadataTabular(metadata) => Ok(metadata.tabular),
            _ => {
                return Err(OxenError::basic_str("Metadata is not tabular"));
            }
        },
        None => {
            return Err(OxenError::basic_str("File node does not have metadata"));
        }
    };
    let metadata = metadata?;

    let source_schema = metadata.schema;
    let data_frame_size = DataFrameSize {
        width: metadata.width,
        height: metadata.height,
    };

    // Read the data frame from the version path
    let version_path = util::fs::version_path_from_hash(repo, file_node.hash.to_string());
    let df = tabular::read_df_with_extension(version_path, file_node.extension, opts)?;

    // Check what the view height is
    let view_height = if opts.has_filter_transform() {
        df.height()
    } else {
        data_frame_size.height
    };

    // Update the schema metadata from the source schema
    let mut slice_schema = Schema::from_polars(&df.schema());
    slice_schema.update_metadata_from_schema(&source_schema);

    // Return a DataFrameSlice
    Ok(DataFrameSlice {
        schemas: DataFrameSliceSchemas {
            source: DataFrameSchemaSize {
                size: data_frame_size,
                schema: source_schema,
            },
            slice: DataFrameSchemaSize {
                size: DataFrameSize {
                    width: df.width(),
                    height: view_height,
                },
                schema: slice_schema,
            },
        },
        slice: df,
    })
}
