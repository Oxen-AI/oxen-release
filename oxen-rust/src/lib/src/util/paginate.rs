use crate::view::Pagination;

/// Returns a vector of entries and the total number of pages.
/// Note: does this in memory, so not as efficient as down at the db level, but rocksdb does not support pagination
pub fn paginate<T: Clone>(entries: Vec<T>, page: usize, page_size: usize) -> (Vec<T>, Pagination) {
    let total = entries.len();
    paginate_with_total(entries, page, page_size, total)
}

/// Returns a vector of entries and the total number of pages.
pub fn paginate_with_total<T: Clone>(
    entries: Vec<T>,
    page_number: usize,
    page_size: usize,
    total_entries: usize,
) -> (Vec<T>, Pagination) {
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    log::debug!(
        "paginate entries page: {} size: {} total: {} total_pages: {}",
        page_number,
        page_size,
        total_entries,
        total_pages,
    );

    let start = if page_number == 0 {
        page_number * page_size
    } else {
        (page_number - 1) * page_size
    };
    let end = start + page_size;

    log::debug!(
        "paginate entries start: {} end: {} total: {}",
        start,
        end,
        entries.len()
    );

    let pagination = Pagination {
        page_size,
        page_number,
        total_pages,
        total_entries,
    };

    if start < entries.len() && end > entries.len() {
        (entries[start..].to_vec(), pagination)
    } else if start < entries.len() && end <= entries.len() {
        (entries[start..end].to_vec(), pagination)
    } else {
        (vec![], pagination)
    }
}
