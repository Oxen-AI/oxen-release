use std::path::PathBuf;

use crate::model::schema::Field;
use crate::util;

#[derive(Debug)]
pub struct AddColVals {
    pub name: String,
    pub value: String,
    pub dtype: String,
}

#[derive(Clone, Debug)]
pub enum DFFilterOp {
    EQ,
    LT,
    GT,
    GTE,
    LTE,
    NEQ,
}

impl DFFilterOp {
    pub fn from_str_op(s: &str) -> DFFilterOp {
        match s {
            "=" => DFFilterOp::EQ,
            "<" => DFFilterOp::LT,
            ">" => DFFilterOp::GT,
            "<=" => DFFilterOp::LTE,
            ">=" => DFFilterOp::GTE,
            "!=" => DFFilterOp::NEQ,
            _ => panic!("Unknown DFFilterOp"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DFFilterOp::EQ => "=",
            DFFilterOp::LT => "<",
            DFFilterOp::GT => ">",
            DFFilterOp::LTE => "<=",
            DFFilterOp::GTE => ">=",
            DFFilterOp::NEQ => "!=",
        }
    }
}

pub struct DFFilter {
    pub op: DFFilterOp,
    pub field: String,
    pub value: String,
}

#[derive(Debug)]
pub struct DFOpts {
    pub output: Option<PathBuf>,
    pub slice: Option<String>,
    pub take: Option<String>,
    pub columns: Option<String>,
    pub filter: Option<String>,
    pub add_col: Option<String>,
    pub add_row: Option<String>,
}

impl DFOpts {
    pub fn empty() -> DFOpts {
        DFOpts {
            output: None,
            slice: None,
            take: None,
            columns: None,
            filter: None,
            add_col: None,
            add_row: None,
        }
    }

    pub fn from_filter_fields(fields: Vec<Field>) -> Self {
        let str_fields: Vec<String> = fields.iter().map(|f| f.name.to_owned()).collect();
        DFOpts {
            output: None,
            slice: None,
            take: None,
            columns: Some(str_fields.join(",")),
            add_col: None,
            filter: None,
            add_row: None,
        }
    }

    pub fn has_transform(&self) -> bool {
        self.slice.is_some()
            || self.take.is_some()
            || self.columns.is_some()
            || self.add_col.is_some()
            || self.add_row.is_some()
            || self.filter.is_some()
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

    pub fn get_filter(&self) -> Option<DFFilter> {
        if let Some(filter) = self.filter.clone() {
            // Order in which we check matters because some ops are substrings of others, put the longest ones first
            let ops = vec![
                DFFilterOp::NEQ,
                DFFilterOp::GTE,
                DFFilterOp::LTE,
                DFFilterOp::EQ,
                DFFilterOp::GT,
                DFFilterOp::LT,
            ];

            for op in ops.iter() {
                log::debug!("Checking op {:?} in filter {}", op, filter);
                if filter.contains(op.as_str()) {
                    let split = util::str::split_and_trim(&filter, op.as_str());
                    return Some(DFFilter {
                        op: op.clone(),
                        field: split[0].to_owned(),
                        value: split[1].to_owned(),
                    });
                }
            }
        }
        None
    }

    pub fn add_col_vals(&self) -> Option<AddColVals> {
        if let Some(add_col) = self.add_col.clone() {
            let split = add_col
                .split(':')
                .map(String::from)
                .collect::<Vec<String>>();
            if split.len() != 3 {
                panic!("Invalid input for col vals. Format: 'name:val:dtype'");
            }

            return Some(AddColVals {
                name: split[0].to_owned(),
                value: split[1].to_owned(),
                dtype: split[2].to_owned(),
            });
        }
        None
    }

    pub fn add_row_vals(&self) -> Option<Vec<String>> {
        if let Some(add_row) = self.add_row.clone() {
            let split = add_row
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }
}
