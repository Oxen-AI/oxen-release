use crate::constants::{DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE};

#[derive(Clone, Debug)]
pub struct PaginateOpts {
    pub page_num: usize,
    pub page_size: usize,
}

// Add default values
impl Default for PaginateOpts {
    fn default() -> Self {
        PaginateOpts {
            page_num: DEFAULT_PAGE_NUM,
            page_size: DEFAULT_PAGE_SIZE,
        }
    }
}
