use std::path::PathBuf;

pub struct DFOpts {
    pub output: Option<PathBuf>,
    pub slice: Option<String>,
    pub take: Option<String>,
    pub columns: Option<String>,
}

impl DFOpts {
    pub fn empty() -> DFOpts {
        DFOpts {
            output: None,
            slice: None,
            take: None,
            columns: None,
        }
    }

    pub fn has_filter(&self) -> bool {
        self.slice.is_some() || self.take.is_some() || self.columns.is_some()
    }

    pub fn slice_indices(&self) -> Option<(i64, i64)> {
        if let Some(slice) = self.slice.clone() {
            let split = slice.split("..").collect::<Vec<&str>>();
            if split.len() == 2 {
                let start = split[0]
                    .parse::<i64>()
                    .expect("Start must be a valid integer.");
                let len = split[1]
                    .parse::<i64>()
                    .expect("End must be a valid integer.");
                return Some((start, len));
            } else {
                return None;
            }
        }
        None
    }

    pub fn take_indices(&self) -> Option<Vec<u32>> {
        if let Some(take) = self.take.clone() {
            let split = take
                .split(',')
                .map(|v| v.parse::<u32>().expect("Values must be a valid u32."))
                .collect::<Vec<u32>>();
            return Some(split);
        }
        None
    }

    pub fn columns_names(&self) -> Option<Vec<String>> {
        if let Some(columns) = self.columns.clone() {
            let split = columns
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }
}
