use crate::model::diff::AddRemoveModifyCounts;
use crate::model::DiffEntry;
use crate::view::Pagination;

pub struct DiffEntriesCounts {
    pub entries: Vec<DiffEntry>,
    pub counts: AddRemoveModifyCounts,
    pub pagination: Pagination,
}
