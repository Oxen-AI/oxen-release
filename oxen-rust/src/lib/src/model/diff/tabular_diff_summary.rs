use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::core::df::tabular;
use crate::model::{CommitEntry, DataFrameSize, LocalRepository};
use crate::opts::DFOpts;
use crate::util;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummary {
    pub num_added_rows: usize,
    pub num_added_cols: usize,
    pub num_removed_rows: usize,
    pub num_removed_cols: usize,
    pub schema_has_changed: bool,
}

impl TabularDiffSummary {
    pub fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
    ) -> TabularDiffSummary {
        let base_df = TabularDiffSummary::maybe_get_df(repo, base_entry);
        let head_df = TabularDiffSummary::maybe_get_df(repo, head_entry);

        let base_size = TabularDiffSummary::maybe_get_size(&base_df);
        let head_size = TabularDiffSummary::maybe_get_size(&head_df);

        let num_added_rows = TabularDiffSummary::maybe_get_added_rows(&base_size, &head_size);
        // removed is just opposite of added
        let num_removed_rows = TabularDiffSummary::maybe_get_added_rows(&head_size, &base_size);
        let num_added_cols = TabularDiffSummary::compute_num_added_cols(&base_df, &head_df);
        let num_removed_cols = TabularDiffSummary::compute_num_removed_cols(&base_df, &head_df);

        let schema_has_changed = TabularDiffSummary::schema_has_changed(&base_df, &head_df);

        TabularDiffSummary {
            num_added_rows,
            num_added_cols,
            num_removed_rows,
            num_removed_cols,
            schema_has_changed,
        }
    }

    pub fn maybe_get_df(repo: &LocalRepository, entry: &Option<CommitEntry>) -> Option<DataFrame> {
        match entry {
            Some(entry) => {
                let version_path = util::fs::version_path(repo, entry);
                match tabular::read_df(version_path, DFOpts::empty()) {
                    Ok(df) => Some(df),
                    Err(_) => None,
                }
            }
            None => None,
        }
    }

    pub fn maybe_get_added_rows(
        base_size: &Option<DataFrameSize>,
        head_size: &Option<DataFrameSize>,
    ) -> usize {
        match (base_size, head_size) {
            (Some(base_size), Some(head_size)) => {
                if base_size.height < head_size.height {
                    head_size.height - base_size.height
                } else {
                    0
                }
            }
            _ => 0,
        }
    }

    pub fn maybe_get_size(df: &Option<DataFrame>) -> Option<DataFrameSize> {
        df.as_ref().map(|df| DataFrameSize {
                height: df.height(),
                width: df.width(),
            })
    }

    pub fn schema_has_changed(base_df: &Option<DataFrame>, head_df: &Option<DataFrame>) -> bool {
        if base_df.is_none() && head_df.is_none() {
            return false;
        }

        if let Some(base_df) = base_df {
            if let Some(head_df) = head_df {
                return TabularDiffSummary::schema_has_changed_df(base_df, head_df);
            }
        }

        // if we get here, one of the dataframes is None and the other is not
        true
    }

    fn schema_has_changed_df(base_df: &DataFrame, head_df: &DataFrame) -> bool {
        let base_schema = base_df.schema();
        let head_schema = head_df.schema();

        // compare the schemas
        base_schema != head_schema
    }

    pub fn compute_num_added_cols(
        base_df: &Option<DataFrame>,
        head_df: &Option<DataFrame>,
    ) -> usize {
        if base_df.is_none() || head_df.is_none() {
            // Both dataframes are empty so no columns were added
            return 0;
        }

        if base_df.is_none() && head_df.is_some() {
            // All columns were added because base is none
            return head_df.as_ref().unwrap().width();
        }

        if base_df.is_some() && head_df.is_none() {
            // All columns were removed because head is none
            return 0;
        }

        let base_schema = base_df.as_ref().unwrap().schema();
        let head_schema = head_df.as_ref().unwrap().schema();

        let head_cols = head_schema.iter_fields();

        let mut num_added_cols = 0;
        for col in head_cols {
            if base_schema.get_field(&col.name).is_none() {
                num_added_cols += 1;
            }
        }

        num_added_cols
    }

    pub fn compute_num_removed_cols(
        base_df: &Option<DataFrame>,
        head_df: &Option<DataFrame>,
    ) -> usize {
        if base_df.is_none() || head_df.is_none() {
            // Both dataframes are empty so no columns were removed
            return 0;
        }

        if base_df.is_none() && head_df.is_some() {
            // All columns were added because base is none
            return 0;
        }

        if base_df.is_some() && head_df.is_none() {
            // All columns were removed because head is none
            return base_df.as_ref().unwrap().width();
        }

        let base_schema = base_df.as_ref().unwrap().schema();
        let head_schema = head_df.as_ref().unwrap().schema();

        let base_cols = base_schema.iter_fields();

        let mut num_removed_cols = 0;
        for col in base_cols {
            if head_schema.get_field(&col.name).is_none() {
                num_removed_cols += 1;
            }
        }

        num_removed_cols
    }
}
