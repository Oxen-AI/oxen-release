use actix_web::web;
use liboxen::opts::DFOpts;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct DFOptsQuery {
    pub columns: Option<String>,
    pub delimiter: Option<String>,
    pub page_size: Option<usize>,
    pub page: Option<usize>,
    pub row: Option<usize>,
    pub randomize: Option<bool>,
    pub reverse: Option<bool>,
    pub slice: Option<String>,
    pub sort_by: Option<String>,
    pub sql: Option<String>,
    pub take: Option<String>,
}

/// Provide some default vals for opts
pub fn parse_opts(query: &web::Query<DFOptsQuery>, filter_ops: &mut DFOpts) -> DFOpts {
    // Default to 0..10 unless they ask for "all"
    log::debug!("Parsing opts {:?}", query);
    if let Some(slice) = query.slice.clone() {
        if slice == "all" {
            // Return everything...probably don't want to do this unless explicitly asked for
            filter_ops.slice = None;
        } else {
            // Return what they asked for
            filter_ops.slice = Some(slice);
        }
    }

    // we are already filtering the hidden columns
    if let Some(columns) = query.columns.clone() {
        filter_ops.columns = Some(columns);
    }

    filter_ops.delimiter.clone_from(&query.delimiter);
    filter_ops.page = query.page;
    filter_ops.page_size = query.page_size;
    filter_ops.row = query.row;
    filter_ops.should_randomize = query.randomize.unwrap_or(false);
    filter_ops.should_reverse = query.reverse.unwrap_or(false);
    filter_ops.sort_by.clone_from(&query.sort_by);
    filter_ops.sql.clone_from(&query.sql);
    filter_ops.take.clone_from(&query.take);

    filter_ops.clone()
}
