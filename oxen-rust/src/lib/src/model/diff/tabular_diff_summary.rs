use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::api;
use crate::core::df::tabular;
use crate::model::{CommitEntry, DataFrameSize, LocalRepository};
use crate::opts::DFOpts;
use crate::util;

// THE DIFFERENCE BETWEEN WRAPPER AND SUMMARY IS JUST THE KEY NAME IN THE JSON RESPONSE
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffWrapper {
    pub tabular: TabularDiffSummaryImpl,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummary {
    pub summary: TabularDiffSummaryImpl,
}

// Impl is so that we can wrap the json response in the "tabular" field to make summaries easier to distinguish
#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiffSummaryImpl {
    pub num_added_rows: usize,
    pub num_added_cols: usize,
    pub num_removed_rows: usize,
    pub num_removed_cols: usize,
    pub schema_has_changed: bool,
}

impl TabularDiffSummary {
    pub fn to_wrapper(&self) -> TabularDiffWrapper {
        TabularDiffWrapper {
            tabular: self.summary.clone(),
        }
    }
}

impl TabularDiffWrapper {
    pub fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
    ) -> TabularDiffWrapper {
        let base_df = TabularDiffWrapper::maybe_get_df(repo, base_entry);
        let head_df = TabularDiffWrapper::maybe_get_df(repo, head_entry);

        let schema_has_changed = TabularDiffWrapper::schema_has_changed(&base_df, &head_df);

        // log::debug!("TabularDiffSummary::from_commit_entries: schema_has_changed: {}", schema_has_changed);
        // log::debug!("TabularDiffSummary::from_commit_entries: base_df: {:?}", base_df);
        // log::debug!("TabularDiffSummary::from_commit_entries: head_df: {:?}", head_df);
        let mut num_added_rows = 0;
        let mut num_removed_rows = 0;
        if !schema_has_changed {
            num_added_rows = TabularDiffWrapper::maybe_count_added_rows(&base_df, &head_df);
            num_removed_rows = TabularDiffWrapper::maybe_count_removed_rows(&base_df, &head_df);
        }

        let num_added_cols = TabularDiffWrapper::compute_num_added_cols(&base_df, &head_df);
        let num_removed_cols = TabularDiffWrapper::compute_num_removed_cols(&base_df, &head_df);

        TabularDiffWrapper {
            tabular: TabularDiffSummaryImpl {
                num_added_rows,
                num_added_cols,
                num_removed_rows,
                num_removed_cols,
                schema_has_changed,
            },
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

    pub fn maybe_count_added_rows(
        base_df: &Option<DataFrame>,
        head_df: &Option<DataFrame>,
    ) -> usize {
        match (base_df, head_df) {
            (Some(base_df), Some(head_df)) => {
                match api::local::diff::count_added_rows(base_df.clone(), head_df.clone()) {
                    Ok(count) => count,
                    Err(err) => {
                        log::error!("Error counting added rows: {}", err);
                        0
                    }
                }
            }
            (None, Some(head_df)) => head_df.height(),
            _ => 0,
        }
    }

    pub fn maybe_count_removed_rows(
        base_df: &Option<DataFrame>,
        head_df: &Option<DataFrame>,
    ) -> usize {
        match (base_df, head_df) {
            (Some(base_df), Some(head_df)) => {
                match api::local::diff::count_removed_rows(base_df.clone(), head_df.clone()) {
                    Ok(count) => count,
                    Err(err) => {
                        log::error!("Error counting added rows: {}", err);
                        0
                    }
                }
            }
            (None, Some(head_df)) => head_df.height(),
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
                return TabularDiffWrapper::schema_has_changed_df(base_df, head_df);
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
