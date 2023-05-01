//! # oxen df
//!
//! Interact with DataFrames
//!

use std::path::Path;

use crate::df::tabular;
use crate::error::OxenError;
use crate::opts::DFOpts;
use crate::util;

/// Interact with DataFrames
pub fn df<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<(), OxenError> {
    let mut df = tabular::show_path(input, opts.clone())?;

    if let Some(output) = opts.output {
        println!("Writing {output:?}");
        tabular::write_df(&mut df, output)?;
    }

    Ok(())
}

/// Get a human readable schema for a DataFrame
pub fn schema<P: AsRef<Path>>(input: P, flatten: bool, opts: DFOpts) -> Result<String, OxenError> {
    tabular::schema_to_string(input, flatten, &opts)
}

/// Add a row to a dataframe
pub fn add_row(path: &Path, data: &str) -> Result<(), OxenError> {
    if util::fs::is_tabular(path) {
        let mut opts = DFOpts::empty();
        opts.add_row = Some(data.to_string());
        opts.output = Some(path.to_path_buf());
        df(path, opts)?;
    } else {
        // TODO: Seems like we don't need to support this...
        util::fs::append_to_file(path, data)?;
    }

    Ok(())
}
