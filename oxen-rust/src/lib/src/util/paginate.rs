/// Returns a vector of entries and the total number of pages.
pub fn paginate<T: Clone>(entries: Vec<T>, page: usize, page_size: usize) -> (Vec<T>, usize) {
    let total_entries = entries.len();
    let total_pages = (total_entries as f64 / page_size as f64).ceil() as usize;
    log::debug!(
        "paginate entries page: {} size: {} total: {} total_pages: {}",
        page,
        page_size,
        total_entries,
        total_pages,
    );

    let start = if page == 0 {
        page * page_size
    } else {
        (page - 1) * page_size
    };
    let end = start + page_size;

    log::debug!(
        "paginate entries start: {} end: {} total: {}",
        start,
        end,
        entries.len()
    );

    if start < entries.len() && end > entries.len() {
        (entries[start..].to_vec(), total_pages)
    } else if start < entries.len() && end <= entries.len() {
        (entries[start..end].to_vec(), total_pages)
    } else {
        (vec![], total_pages)
    }
}
