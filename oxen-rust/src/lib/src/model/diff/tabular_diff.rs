use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

use crate::api;
use crate::model::{CommitEntry, LocalRepository, Schema};
use crate::opts::PaginateOpts;
use crate::view::JsonDataFrame;

use super::tabular_diff_summary::{TabularDiffSummary, TabularDiffSummaryImpl};

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct TabularDiff {
    #[serde(flatten)]
    pub summary: TabularDiffSummary,

    pub base_schema: Option<Schema>,
    pub head_schema: Option<Schema>,

    pub added_rows: Option<JsonDataFrame>,
    pub removed_rows: Option<JsonDataFrame>,
    pub added_cols: Option<JsonDataFrame>,
    pub removed_cols: Option<JsonDataFrame>,
}

impl TabularDiff {
    pub fn from_commit_entries(
        repo: &LocalRepository,
        base_entry: &Option<CommitEntry>,
        head_entry: &Option<CommitEntry>,
        pagination: PaginateOpts,
    ) -> TabularDiff {
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

        let summary = TabularDiffSummary {
            tabular: TabularDiffSummaryImpl {
                num_added_rows,
                num_added_cols,
                num_removed_rows,
                num_removed_cols,
                schema_has_changed,
            },
        };

        let base_schema = TabularDiff::maybe_get_schema(&base_df);
        let head_schema = TabularDiff::maybe_get_schema(&head_df);

        if schema_has_changed {
            if base_schema.is_some() && head_schema.is_some() {
                let base_schema = base_schema.clone().unwrap();
                let head_schema = head_schema.clone().unwrap();
                let base_df = base_df.unwrap();
                let head_df = head_df.unwrap();

                if head_schema.hash != base_schema.hash {
                    // compute new columns
                    let df_diff = api::local::diff::compute_new_columns_from_dfs(
                        base_df,
                        head_df,
                        &base_schema,
                        &head_schema,
                    )
                    .unwrap();

                    let added_cols = df_diff
                        .added_cols
                        .map(|df| JsonDataFrame::from_df_paginated(df, &pagination));
                    let removed_cols = df_diff
                        .removed_cols
                        .map(|df| JsonDataFrame::from_df_paginated(df, &pagination));

                    return TabularDiff {
                        summary,
                        base_schema: Some(base_schema),
                        head_schema: Some(head_schema),
                        added_rows: None,
                        removed_rows: None,
                        added_cols,
                        removed_cols,
                    };
                } else {
                    // compute new rows
                    let df_diff =
                        api::local::diff::compute_new_rows(base_df, head_df, &base_schema).unwrap();

                    let added_rows = df_diff
                        .added_rows
                        .map(|df| JsonDataFrame::from_df_paginated(df, &pagination));
                    let removed_rows = df_diff
                        .removed_rows
                        .map(|df| JsonDataFrame::from_df_paginated(df, &pagination));

                    return TabularDiff {
                        summary,
                        base_schema: Some(base_schema),
                        head_schema: Some(head_schema),
                        added_rows,
                        removed_rows,
                        added_cols: None,
                        removed_cols: None,
                    };
                }
            }

            if base_schema.is_none() && head_schema.is_some() {
                // we added the dataframe
                let head_schema = head_schema.clone().unwrap();
                let head_df = head_df.unwrap();
                let added_df = Some(JsonDataFrame::from_df_paginated(head_df, &pagination));

                return TabularDiff {
                    summary,
                    base_schema: None,
                    head_schema: Some(head_schema),
                    added_rows: added_df,
                    removed_rows: None,
                    added_cols: None,
                    removed_cols: None,
                };
            }

            if base_schema.is_some() && head_schema.is_none() {
                // we removed the dataframe
                let base_schema = base_schema.clone().unwrap();
                let base_df = base_df.unwrap();
                let removed_df = Some(JsonDataFrame::from_df_paginated(base_df, &pagination));

                return TabularDiff {
                    summary,
                    base_schema: Some(base_schema),
                    head_schema: None,
                    added_rows: None,
                    removed_rows: removed_df,
                    added_cols: None,
                    removed_cols: None,
                };
            }
        }

        // schema has not changed
        TabularDiff {
            summary,
            base_schema,
            head_schema,
            added_rows: None,
            removed_rows: None,
            added_cols: None,
            removed_cols: None,
        }
    }

    pub fn maybe_get_schema(df: &Option<DataFrame>) -> Option<Schema> {
        df.as_ref().map(|df| Schema::from_polars(&df.schema()))
    }
}
