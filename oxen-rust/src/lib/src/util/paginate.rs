pub fn paginate<T: Clone>(entries: Vec<T>, page: usize, page_size: usize) -> Vec<T> {
    log::debug!(
        "paginate entries page: {} size: {} total: {}",
        page,
        page_size,
        entries.len()
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
        entries[start..].to_vec()
    } else if start < entries.len() && end <= entries.len() {
        entries[start..end].to_vec()
    } else {
        vec![]
    }
}
