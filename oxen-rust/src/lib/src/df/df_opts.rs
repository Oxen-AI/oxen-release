use std::path::PathBuf;

use crate::constants::{
    DEFAULT_PAGE_NUM, DEFAULT_PAGE_SIZE, FILE_ROW_NUM_COL_NAME, ROW_HASH_COL_NAME, ROW_NUM_COL_NAME,
};
use crate::df::agg::{self, DFAggregation};
use crate::error::OxenError;
use crate::model::schema::Field;
use crate::model::Schema;

use super::filter::{self, DFFilterExp};

#[derive(Debug)]
pub struct AddColVals {
    pub name: String,
    pub value: String,
    pub dtype: String,
}

#[derive(Clone, Debug)]
pub struct IndexedItem {
    pub col: String,
    pub index: usize,
}

#[derive(Clone, Debug)]
pub struct DFOpts {
    pub output: Option<PathBuf>,
    pub slice: Option<String>,
    pub take: Option<String>,
    pub columns: Option<String>,
    pub filter: Option<String>,
    pub aggregate: Option<String>,
    pub col_at: Option<String>,
    pub vstack: Option<Vec<PathBuf>>,
    pub add_col: Option<String>,
    pub add_row: Option<String>,
    pub sort_by: Option<String>,
    pub unique: Option<String>,
    pub should_randomize: bool,
    pub should_reverse: bool,
    pub page: Option<usize>,
    pub page_size: Option<usize>,
}

impl DFOpts {
    pub fn empty() -> DFOpts {
        DFOpts {
            output: None,
            slice: None,
            take: None,
            columns: None,
            filter: None,
            aggregate: None,
            col_at: None,
            vstack: None,
            add_col: None,
            add_row: None,
            sort_by: None,
            unique: None,
            should_randomize: false,
            should_reverse: false,
            page: None,
            page_size: None,
        }
    }

    pub fn from_agg(query: &str) -> Self {
        let mut opts = DFOpts::empty();
        opts.aggregate = Some(String::from(query));
        opts
    }

    pub fn from_filter_query(query: &str) -> Self {
        let mut opts = DFOpts::empty();
        opts.filter = Some(String::from(query));
        opts
    }

    pub fn from_unique(fields_str: &str) -> Self {
        let mut opts = DFOpts::empty();
        opts.unique = Some(String::from(fields_str));
        opts
    }

    pub fn from_schema_columns(schema: &Schema) -> Self {
        DFOpts::from_columns(schema.fields.clone())
    }

    pub fn from_schema_columns_exclude_hidden(schema: &Schema) -> Self {
        let fields: Vec<Field> = schema
            .fields
            .clone()
            .into_iter()
            .filter(|f| {
                f.name != ROW_HASH_COL_NAME
                    && f.name != ROW_NUM_COL_NAME
                    && f.name != FILE_ROW_NUM_COL_NAME
            })
            .collect();
        DFOpts::from_columns(fields)
    }

    pub fn from_columns(fields: Vec<Field>) -> Self {
        let str_fields: Vec<String> = fields.iter().map(|f| f.name.to_owned()).collect();
        let mut opts = DFOpts::empty();
        opts.columns = Some(str_fields.join(","));
        opts
    }

    pub fn has_transform(&self) -> bool {
        self.slice.is_some()
            || self.page.is_some()
            || self.page_size.is_some()
            || self.take.is_some()
            || self.columns.is_some()
            || self.vstack.is_some()
            || self.add_col.is_some()
            || self.add_row.is_some()
            || self.filter.is_some()
            || self.aggregate.is_some()
            || self.col_at.is_some()
            || self.sort_by.is_some()
            || self.unique.is_some()
            || self.should_randomize
            || self.should_reverse
    }

    pub fn slice_indices(&self) -> Option<(i64, i64)> {
        if let Some(slice) = self.slice.clone() {
            let split = slice.split("..").collect::<Vec<&str>>();
            if split.len() == 2 {
                let start = split[0]
                    .parse::<i64>()
                    .expect("Start must be a valid integer.");
                let end = split[1]
                    .parse::<i64>()
                    .expect("End must be a valid integer.");
                return Some((start, end));
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

    pub fn unique_columns(&self) -> Option<Vec<String>> {
        if let Some(columns) = self.unique.clone() {
            let split = columns
                .split(',')
                .map(String::from)
                .collect::<Vec<String>>();
            return Some(split);
        }
        None
    }

    pub fn get_filter(&self) -> Result<Option<DFFilterExp>, OxenError> {
        filter::parse(self.filter.clone())
    }

    /// Parse and return the aggregation if it exists
    /// 'col_to_agg' -> ('col_1', min('col_2'), n_unique('col_3'))
    /// returns error if not a valid query
    pub fn get_aggregation(&self) -> Result<Option<DFAggregation>, OxenError> {
        if let Some(query) = self.aggregate.clone() {
            let agg = agg::parse_query(&query)?;
            return Ok(Some(agg));
        }
        Ok(None)
    }

    pub fn column_at(&self) -> Option<IndexedItem> {
        if let Some(value) = self.col_at.clone() {
            // col:index
            // ie: file:2
            let delimiter = ":";
            if value.contains(delimiter) {
                let mut split = value.split(delimiter);
                return Some(IndexedItem {
                    col: String::from(split.next().unwrap()),
                    index: split
                        .next()
                        .unwrap()
                        .parse::<usize>()
                        .expect("Index must be usize"),
                });
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

    pub fn to_http_query_params(&self) -> String {
        let randomize = if self.should_randomize {
            Some(String::from("true"))
        } else {
            Some(String::from("false"))
        };
        let should_reverse = if self.should_reverse {
            Some(String::from("true"))
        } else {
            Some(String::from("false"))
        };
        let page = if self.page.is_some() {
            Some(format!("{}", self.page.unwrap()))
        } else {
            Some(format!("{}", DEFAULT_PAGE_NUM))
        };
        let page_size = if self.page_size.is_some() {
            Some(format!("{}", self.page_size.unwrap()))
        } else {
            Some(format!("{}", DEFAULT_PAGE_SIZE))
        };
        let params = vec![
            ("slice", self.slice.clone()),
            ("take", self.take.clone()),
            ("columns", self.columns.clone()),
            ("filter", self.filter.clone()),
            ("aggregate", self.aggregate.clone()),
            ("col-at", self.col_at.clone()),
            ("sort-by", self.sort_by.clone()),
            ("unique", self.unique.clone()),
            ("randomize", randomize),
            ("reverse", should_reverse),
            ("page", page),
            ("page_size", page_size),
        ];

        let mut query = String::new();
        for (name, val) in params {
            if let Some(val) = val {
                query.push_str(&format!("{}={}&", name, urlencoding::encode(&val)));
            }
        }
        query
    }
}

#[cfg(test)]
mod tests {
    use crate::{df::DFOpts, error::OxenError};

    #[test]
    fn test_parse_agg_one_lit_input_one_output() -> Result<(), OxenError> {
        let agg_query = "('col_0') -> (list('col_1'))";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation()?.unwrap();

        // Make sure group_by is correct
        assert_eq!(agg_opt.group_by.len(), 1);
        assert_eq!(agg_opt.group_by[0], "col_0");

        // Make sure agg is correct
        assert_eq!(agg_opt.agg.len(), 1);
        assert_eq!(agg_opt.agg[0].name, "list");
        assert_eq!(agg_opt.agg[0].args.len(), 1);
        assert_eq!(agg_opt.agg[0].args[0], "col_1");
        Ok(())
    }

    #[test]
    fn test_parse_double_quotes() -> Result<(), OxenError> {
        let agg_query = "(\"col_0\") -> (count(\"col_1\"))";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation()?.unwrap();

        // Make sure group_by is correct
        assert_eq!(agg_opt.group_by.len(), 1);
        assert_eq!(agg_opt.group_by[0], "col_0");

        // Make sure agg is correct
        assert_eq!(agg_opt.agg.len(), 1);
        assert_eq!(agg_opt.agg[0].name, "count");
        assert_eq!(agg_opt.agg[0].args.len(), 1);
        assert_eq!(agg_opt.agg[0].args[0], "col_1");
        Ok(())
    }

    #[test]
    fn test_parse_agg_two_lit_input_one_output() -> Result<(), OxenError> {
        let agg_query = "('col_0', 'col_2') -> (list('col_1'))";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation()?.unwrap();

        // Make sure group_by is correct
        assert_eq!(agg_opt.group_by.len(), 2);
        assert_eq!(agg_opt.group_by[0], "col_0");
        assert_eq!(agg_opt.group_by[1], "col_2");

        // Make sure agg is correct
        assert_eq!(agg_opt.agg.len(), 1);
        assert_eq!(agg_opt.agg[0].name, "list");
        assert_eq!(agg_opt.agg[0].args.len(), 1);
        assert_eq!(agg_opt.agg[0].args[0], "col_1");
        Ok(())
    }

    #[test]
    fn test_parse_agg_two_lit_input_three_output() -> Result<(), OxenError> {
        let agg_query = "('col_0', 'col_2') -> (list('col_3'), max('col_2'), n_unique('col_1'))";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation()?.unwrap();

        // Make sure group_by is correct
        assert_eq!(agg_opt.group_by.len(), 2);
        assert_eq!(agg_opt.group_by[0], "col_0");
        assert_eq!(agg_opt.group_by[1], "col_2");

        // Make sure agg is correct
        assert_eq!(agg_opt.agg.len(), 3);
        assert_eq!(agg_opt.agg[0].name, "list");
        assert_eq!(agg_opt.agg[0].args.len(), 1);
        assert_eq!(agg_opt.agg[0].args[0], "col_3");

        assert_eq!(agg_opt.agg[1].name, "max");
        assert_eq!(agg_opt.agg[1].args.len(), 1);
        assert_eq!(agg_opt.agg[1].args[0], "col_2");

        assert_eq!(agg_opt.agg[2].name, "n_unique");
        assert_eq!(agg_opt.agg[2].args.len(), 1);
        assert_eq!(agg_opt.agg[2].args[0], "col_1");
        Ok(())
    }

    #[test]
    fn test_parse_agg_invalid_empty() -> Result<(), OxenError> {
        let agg_query = "";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation();
        assert!(agg_opt.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_agg_invalid_string() -> Result<(), OxenError> {
        let agg_query = "this shouldn't work";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation();
        assert!(agg_opt.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_agg_invalid_starts_with_paren() -> Result<(), OxenError> {
        let agg_query = "(this shouldn't work";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation();
        assert!(agg_opt.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_agg_invalid_no_closed_single_quotes() -> Result<(), OxenError> {
        let agg_query = "(this shouldn't work)";

        let opts = DFOpts::from_agg(agg_query);
        let agg_opt = opts.get_aggregation();
        assert!(agg_opt.is_err());
        Ok(())
    }

    #[test]
    fn test_parse_filter_single_query() -> Result<(), OxenError> {
        let query = "";

        let opts = DFOpts::from_filter_query(query);
        let agg_opt = opts.get_filter();
        assert!(agg_opt.is_err());
        Ok(())
    }
}
