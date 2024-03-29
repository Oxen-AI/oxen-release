// use crate::model::diff::dir_diff::DirDiff;
use crate::model::diff::tabular_diff::TabularDiff;
use crate::model::diff::text_diff::TextDiff;

#[derive(Debug, Clone)]
pub enum DiffResult {
    Tabular(TabularDiff),
    Text(TextDiff),
}
