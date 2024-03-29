// This is the new dataframe format, depreciate JsonDataFrameSliceResponse

use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use super::StatusMessage;
use crate::constants;
use crate::core::df::tabular;
use crate::model::Commit;
use crate::model::DataFrameSize;
use crate::opts::df_opts::DFOptsView;

use crate::view::entry::ResourceVersion;
use crate::view::Pagination;
use crate::{model::Schema, opts::DFOpts};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameSource {
    pub schema: Schema,
    pub size: DataFrameSize,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameView {
    pub schema: Schema,
    pub size: DataFrameSize,
    pub data: serde_json::Value,
    pub pagination: Pagination,
    #[serde(flatten)]
    pub opts: DFOptsView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViews {
    pub source: JsonDataFrameSource,
    pub view: JsonDataFrameView,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameViewResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct JsonDataFrameRowResponse {
    #[serde(flatten)]
    pub status: StatusMessage,
    pub data_frame: JsonDataFrameViews,
    pub commit: Option<Commit>,
    pub resource: Option<ResourceVersion>,
    pub derived_resource: Option<DerivedDFResource>,
    pub row_id: Option<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum DFResourceType {
    Compare,
    Diff,
    Query,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DerivedDFResource {
    pub resource_id: String,
    pub path: String,
    pub resource_type: DFResourceType,
}

impl JsonDataFrameSource {
    pub fn from_df_size(data_frame_size: &DataFrameSize, schema: &Schema) -> JsonDataFrameSource {
        JsonDataFrameSource {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: data_frame_size.height,
                width: data_frame_size.width,
            },
        }
    }
    pub fn from_df(df: &DataFrame, schema: &Schema) -> JsonDataFrameSource {
        JsonDataFrameSource {
            schema: schema.to_owned(),
            size: DataFrameSize {
                height: df.height(),
                width: df.width(),
            },
        }
    }
}

impl JsonDataFrameView {
    pub fn from_df_opts(df: DataFrame, og_schema: Schema, opts: &DFOpts) -> JsonDataFrameView {
        let full_width = df.width();
        let full_height = df.height();

        let page_size = opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
        let page = opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM);

        let start = if page == 0 { 0 } else { page_size * (page - 1) };
        let end = page_size * page;

        let total_pages = (full_height as f64 / page_size as f64).ceil() as usize;

        let mut opts = opts.clone();
        opts.slice = Some(format!("{}..{}", start, end));
        let opts_view = DFOptsView::from_df_opts(&opts);
        let mut sliced_df = tabular::transform(df, opts).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        slice_schema.update_metadata_from_schema(&og_schema);

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_from_df(&mut sliced_df),
            pagination: Pagination {
                page_number: page,
                page_size,
                total_pages,
                total_entries: full_height,
            },
            opts: opts_view,
        }
    }

    pub fn from_df_opts_unpaginated(
        df: DataFrame,
        og_schema: Schema,
        og_height: usize,
        opts: &DFOpts,
    ) -> JsonDataFrameView {
        let full_width = df.width();
        let full_height = og_height;

        let opts_view = DFOptsView::from_df_opts(opts);
        let mut sliced_df = tabular::transform(df, opts.clone()).unwrap();

        // Merge the metadata from the original schema
        let mut slice_schema = Schema::from_polars(&sliced_df.schema());
        log::debug!("OG schema {:?}", og_schema);
        log::debug!("Pre-Slice schema {:?}", slice_schema);
        slice_schema.update_metadata_from_schema(&og_schema);
        log::debug!("Slice schema {:?}", slice_schema);

        JsonDataFrameView {
            schema: slice_schema,
            size: DataFrameSize {
                height: full_height,
                width: full_width,
            },
            data: JsonDataFrameView::json_from_df(&mut sliced_df),
            pagination: Pagination {
                page_number: opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM),
                page_size: opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE),
                total_pages: (full_height as f64 / og_height as f64).ceil() as usize,
                total_entries: full_height,
            },
            opts: opts_view,
        }
    }

    pub fn to_df(&self) -> DataFrame {
        if self.data == serde_json::Value::Null {
            DataFrame::empty()
        } else {
            // The fields were coming out of order, so we need to reorder them
            let columns = self.schema.fields_names();
            log::debug!("Got columns: {:?}", columns);

            match &self.data {
                serde_json::Value::Array(arr) => {
                    if !arr.is_empty() {
                        let data = self.data.to_string();
                        let content = Cursor::new(data.as_bytes());
                        log::debug!("Deserializing df: [{}]", data);
                        let df = JsonReader::new(content).finish().unwrap();

                        let opts = DFOpts::from_column_names(columns);
                        tabular::transform(df, opts).unwrap()
                    } else {
                        let cols = columns
                            .iter()
                            .map(|name| Series::new(name, Vec::<&str>::new()))
                            .collect::<Vec<Series>>();
                        DataFrame::new(cols).unwrap()
                    }
                }
                _ => {
                    log::error!("Could not parse non-array json data: {:?}", self.data);
                    DataFrame::empty()
                }
            }
        }
    }

    pub fn json_from_df(df: &mut DataFrame) -> serde_json::Value {
        log::debug!("Serializing df: [{}]", df);

        // TODO: catch errors
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");

        let json_str = str::from_utf8(&buffer).unwrap();

        serde_json::from_str(json_str).unwrap()
    }
}

impl JsonDataFrameViews {
    pub fn from_df_and_opts(df: DataFrame, og_schema: Schema, opts: &DFOpts) -> JsonDataFrameViews {
        let source = JsonDataFrameSource::from_df(&df, &og_schema);
        let view = JsonDataFrameView::from_df_opts(df, og_schema, opts);
        JsonDataFrameViews { source, view }
    }

    // To avoid duplicate pagination when the pagination has already been applied
    // most commonly from duckdb / other sql
    pub fn from_df_and_opts_unpaginated(
        df: DataFrame,
        og_schema: Schema,
        og_height: usize,
        opts: &DFOpts,
    ) -> JsonDataFrameViews {
        let source = JsonDataFrameSource::from_df(&df, &og_schema);
        let view = JsonDataFrameView::from_df_opts_unpaginated(df, og_schema, og_height, opts);
        JsonDataFrameViews { source, view }
    }
}
