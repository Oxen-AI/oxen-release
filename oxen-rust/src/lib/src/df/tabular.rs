use polars::{lazy::dsl::Expr, prelude::*};

use crate::df::df_opts::DFOpts;
use crate::error::OxenError;
use crate::model::schema::DataType;
use crate::model::ContentType;
use crate::util::hasher;
use crate::{constants, df::filter::DFLogicalOp};

use colored::Colorize;
use comfy_table::Table;
use indicatif::ProgressBar;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;

use super::{
    agg::{DFAggFn, DFAggFnType, DFAggregation},
    filter::{DFFilterExp, DFFilterOp, DFFilterVal},
};

const DEFAULT_INFER_SCHEMA_LEN: usize = 10000;
const READ_ERROR: &str = "Could not read tabular data from path";
const COLLECT_ERROR: &str = "Could not collect DataFrame";
const TAKE_ERROR: &str = "Could not take DataFrame";
const CSV_READ_ERROR: &str = "Could not read csv from path";

fn try_infer_schema_csv(reader: CsvReader<File>, delimiter: u8) -> Result<DataFrame, OxenError> {
    log::debug!("try_infer_schema_csv delimiter: {:?}", delimiter as char);
    let result = reader
        .infer_schema(Some(DEFAULT_INFER_SCHEMA_LEN))
        .has_header(true)
        .with_delimiter(delimiter)
        .with_encoding(CsvEncoding::LossyUtf8)
        .finish();

    match result {
        Ok(df) => Ok(df),
        Err(err) => {
            let warning = "Consider specifying a schema for the dtypes.".yellow();
            let suggestion = "You can set a schema for a file with: \n\n  oxen schemas set <file> \"col_name_1:dtype,col_name_2:dtype\" \n";
            log::warn!("Warn: {warning}\n\n{suggestion}");

            let err = format!("{CSV_READ_ERROR}: {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub fn read_df_csv<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<DataFrame, OxenError> {
    match CsvReader::from_path(path.as_ref()) {
        Ok(reader) => Ok(try_infer_schema_csv(reader, delimiter)?),
        Err(err) => {
            let err = format!("{CSV_READ_ERROR}: {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

pub fn scan_df_csv<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<LazyFrame, OxenError> {
    // TODO: The LazyCsvReader was acting funky here on certain csvs...
    let df = read_df_csv(path, delimiter)?;
    Ok(df.lazy())
}

pub fn read_df_json<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let error_str = format!("Could not read json data from path {path:?}");
    let file = File::open(path)?;
    let df = JsonReader::new(file)
        .infer_schema_len(Some(DEFAULT_INFER_SCHEMA_LEN))
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn read_df_jsonl<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let error_str = format!("Could not read line delimited data from path {path:?}");
    let file = File::open(path)?;
    let df = JsonLineReader::new(file)
        .infer_schema_len(Some(DEFAULT_INFER_SCHEMA_LEN))
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn scan_df_jsonl<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyJsonLineReader::new(
        path.as_ref()
            .to_str()
            .expect("Invalid json path.")
            .to_string(),
    )
    .with_infer_schema_length(Some(DEFAULT_INFER_SCHEMA_LEN))
    .finish()
    .unwrap_or_else(|_| panic!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn read_df_parquet<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    let error_str = format!("Could not read tabular data from path {path:?}");
    let file = File::open(path)?;
    let df = ParquetReader::new(file).finish().expect(&error_str);
    Ok(df)
}

pub fn scan_df_parquet<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyFrame::scan_parquet(&path, ScanArgsParquet::default())
        .unwrap_or_else(|_| panic!("{}: {:?}", READ_ERROR, path.as_ref())))
}

fn read_df_arrow<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let file = File::open(path.as_ref())?;
    Ok(IpcReader::new(file)
        .finish()
        .unwrap_or_else(|_| panic!("{}: {:?}", READ_ERROR, path.as_ref())))
}

fn scan_df_arrow<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyFrame::scan_ipc(&path, ScanArgsIpc::default())
        .unwrap_or_else(|_| panic!("{}: {:?}", READ_ERROR, path.as_ref())))
}

pub fn take(df: LazyFrame, indices: Vec<u32>) -> Result<DataFrame, OxenError> {
    let idx = IdxCa::new("idx", &indices);
    let collected = df.collect().expect(COLLECT_ERROR);
    // log::debug!("take indices {:?}", indices);
    // log::debug!("from df {:?}", collected);
    Ok(collected.take(&idx).expect(TAKE_ERROR))
}

pub fn add_col_lazy(
    df: LazyFrame,
    name: &str,
    val: &str,
    dtype: &str,
) -> Result<LazyFrame, OxenError> {
    let mut df = df.collect().expect(COLLECT_ERROR);

    let dtype = DataType::from_string(dtype).to_polars();

    let column = Series::new_empty(name, &dtype);
    let column = column
        .extend_constant(val_from_str_and_dtype(val, &dtype), df.height())
        .expect("Could not extend df");
    df.with_column(column).expect(COLLECT_ERROR);
    let df = df.lazy();
    Ok(df)
}

pub fn add_col(
    mut df: DataFrame,
    name: &str,
    val: &str,
    dtype: &str,
) -> Result<DataFrame, OxenError> {
    let dtype = DataType::from_string(dtype).to_polars();

    let column = Series::new_empty(name, &dtype);
    let column = column
        .extend_constant(val_from_str_and_dtype(val, &dtype), df.height())
        .expect("Could not extend df");
    df.with_column(column).expect(COLLECT_ERROR);
    Ok(df)
}

pub fn add_row(df: LazyFrame, data: String, opts: &DFOpts) -> Result<LazyFrame, OxenError> {
    let df = df.collect().expect(COLLECT_ERROR);

    let schema = crate::model::Schema::from_polars(&df.schema());
    let new_row = parse_data_into_df(&data, &schema, opts.content_type.to_owned())?;
    let df = df.vstack(&new_row).unwrap().lazy();
    Ok(df)
}

pub fn parse_data_into_df(
    data: &str,
    schema: &crate::model::Schema,
    content_type: ContentType,
) -> Result<DataFrame, OxenError> {
    log::debug!("Parsing content into df: {content_type:?}\n{data}");
    match content_type {
        ContentType::Json => {
            // getting an internal error if not jsonl, so do a quick check that it starts with a '{'
            if !data.trim().starts_with('{') {
                return Err(OxenError::basic_str(format!(
                    "Invalid json content: {data}"
                )));
            }

            let cursor = Cursor::new(data.as_bytes());
            match JsonLineReader::new(cursor).finish() {
                Ok(df) => Ok(df),
                Err(err) => Err(OxenError::basic_str(format!(
                    "Error parsing {content_type:?}: {err}"
                ))),
            }
        }
        ContentType::Csv => {
            let fields = schema.fields_to_csv();
            let data = format!("{}\n{}", fields, data);
            let cursor = Cursor::new(data.as_bytes());
            let schema = schema.to_polars();
            match CsvReader::new(cursor).with_schema(&schema).finish() {
                Ok(df) => Ok(df),
                Err(err) => Err(OxenError::basic_str(format!(
                    "Error parsing {content_type:?}: {err}"
                ))),
            }
        }
    }
}

fn val_from_str_and_dtype<'a>(s: &'a str, dtype: &polars::prelude::DataType) -> AnyValue<'a> {
    match dtype {
        polars::prelude::DataType::Boolean => {
            AnyValue::Boolean(s.parse::<bool>().expect("val must be bool"))
        }
        polars::prelude::DataType::UInt8 => AnyValue::UInt8(s.parse::<u8>().expect("must be u8")),
        polars::prelude::DataType::UInt16 => {
            AnyValue::UInt16(s.parse::<u16>().expect("must be u16"))
        }
        polars::prelude::DataType::UInt32 => {
            AnyValue::UInt32(s.parse::<u32>().expect("must be u32"))
        }
        polars::prelude::DataType::UInt64 => {
            AnyValue::UInt64(s.parse::<u64>().expect("must be u64"))
        }
        polars::prelude::DataType::Int8 => AnyValue::Int8(s.parse::<i8>().expect("must be i8")),
        polars::prelude::DataType::Int16 => AnyValue::Int16(s.parse::<i16>().expect("must be i16")),
        polars::prelude::DataType::Int32 => AnyValue::Int32(s.parse::<i32>().expect("must be i32")),
        polars::prelude::DataType::Int64 => AnyValue::Int64(s.parse::<i64>().expect("must be i64")),
        polars::prelude::DataType::Float32 => {
            AnyValue::Float32(s.parse::<f32>().expect("must be f32"))
        }
        polars::prelude::DataType::Float64 => {
            AnyValue::Float64(s.parse::<f64>().expect("must be f64"))
        }
        polars::prelude::DataType::Utf8 => AnyValue::Utf8(s),
        polars::prelude::DataType::Null => AnyValue::Null,
        _ => panic!("Currently do not support data type {}", dtype),
    }
}

fn val_from_df_and_filter<'a>(df: &'a LazyFrame, filter: &'a DFFilterVal) -> AnyValue<'a> {
    if let Some(value) = df
        .schema()
        .expect("Unable to get schema from data frame")
        .iter_fields()
        .find(|f| f.name == filter.field)
    {
        val_from_str_and_dtype(&filter.value, value.data_type())
    } else {
        log::error!("Unknown field {:?}", filter.field);
        AnyValue::Null
    }
}

fn lit_from_any(value: &AnyValue) -> Expr {
    match value {
        AnyValue::Boolean(val) => lit(*val),
        AnyValue::Float64(val) => lit(*val),
        AnyValue::Float32(val) => lit(*val),
        AnyValue::Int64(val) => lit(*val),
        AnyValue::Int32(val) => lit(*val),
        AnyValue::Utf8(val) => lit(*val),
        val => panic!("Unknown data type for [{}] to create literal", val),
    }
}

fn filter_from_val(df: &LazyFrame, filter: &DFFilterVal) -> Expr {
    let val = val_from_df_and_filter(df, filter);
    let val = lit_from_any(&val);
    match filter.op {
        DFFilterOp::EQ => col(&filter.field).eq(val),
        DFFilterOp::GT => col(&filter.field).gt(val),
        DFFilterOp::LT => col(&filter.field).lt(val),
        DFFilterOp::GTE => col(&filter.field).gt_eq(val),
        DFFilterOp::LTE => col(&filter.field).lt_eq(val),
        DFFilterOp::NEQ => col(&filter.field).neq(val),
    }
}

fn filter_df(df: LazyFrame, filter: &DFFilterExp) -> Result<LazyFrame, OxenError> {
    log::debug!("Got filter: {:?}", filter);
    let mut vals = filter.vals.iter();
    let mut expr: Expr = filter_from_val(&df, vals.next().unwrap());
    for op in &filter.logical_ops {
        let chain_expr: Expr = filter_from_val(&df, vals.next().unwrap());

        match op {
            DFLogicalOp::AND => expr = expr.and(chain_expr),
            DFLogicalOp::OR => expr = expr.or(chain_expr),
        }
    }

    Ok(df.filter(expr))
}

fn agg_fn_to_expr(agg: &DFAggFn) -> Result<Expr, OxenError> {
    let col_name = &agg.args[0];
    match DFAggFnType::from_fn_name(&agg.name) {
        DFAggFnType::List => Ok(col(col_name).alias(&format!("list('{col_name}')"))),
        DFAggFnType::Count => Ok(col(col_name).count().alias(&format!("count('{col_name}')"))),
        DFAggFnType::NUnique => Ok(col(col_name)
            .n_unique()
            .alias(&format!("n_unique('{col_name}')"))),
        DFAggFnType::Min => Ok(col(col_name).min().alias(&format!("min('{col_name}')"))),
        DFAggFnType::Max => Ok(col(col_name).max().alias(&format!("max('{col_name}')"))),
        DFAggFnType::ArgMin => Ok(col(col_name)
            .arg_min()
            .alias(&format!("arg_min('{col_name}')"))),
        DFAggFnType::ArgMax => Ok(col(col_name).arg_max().alias(&format!("max('{col_name}')"))),
        DFAggFnType::Mean => Ok(col(col_name).mean().alias(&format!("mean('{col_name}')"))),
        DFAggFnType::Median => Ok(col(col_name)
            .median()
            .alias(&format!("median('{col_name}')"))),
        DFAggFnType::Std => Ok(col(col_name).std(0).alias(&format!("std('{col_name}')"))),
        DFAggFnType::Var => Ok(col(col_name).var(0).alias(&format!("var('{col_name}')"))),
        DFAggFnType::First => Ok(col(col_name).first().alias(&format!("first('{col_name}')"))),
        DFAggFnType::Last => Ok(col(col_name).last().alias(&format!("last('{col_name}')"))),
        DFAggFnType::Head => Ok(col(col_name)
            .head(Some(5))
            .alias(&format!("head('{col_name}', 5)"))),
        DFAggFnType::Tail => Ok(col(col_name)
            .tail(Some(5))
            .alias(&format!("tail('{col_name}', 5)"))),
        DFAggFnType::Unknown => Err(OxenError::unknown_agg_fn(&agg.name)),
    }
}

fn aggregate_df(df: LazyFrame, aggregation: &DFAggregation) -> Result<LazyFrame, OxenError> {
    log::debug!("Got agg: {:?}", aggregation);

    let group_by: Vec<Expr> = aggregation.group_by.iter().map(|c| col(c)).collect();
    let agg: Vec<Expr> = aggregation
        .agg
        .iter()
        .map(|f| agg_fn_to_expr(f).expect("Err:"))
        .collect();

    Ok(df.groupby(group_by).agg(agg))
}

fn unique_df(df: LazyFrame, columns: Vec<String>) -> Result<LazyFrame, OxenError> {
    log::debug!("Got unique: {:?}", columns);
    Ok(df.unique(Some(columns), UniqueKeepStrategy::First))
}

pub fn transform(df: DataFrame, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let height = df.height();
    transform_lazy(df.lazy(), height, opts)
}

pub fn transform_lazy(
    mut df: LazyFrame,
    height: usize,
    opts: DFOpts,
) -> Result<DataFrame, OxenError> {
    log::debug!("Got transform ops {:?}", opts);

    if let Some(vstack) = &opts.vstack {
        log::debug!("Got files to stack {:?}", vstack);
        for path in vstack.iter() {
            let opts = DFOpts::empty();
            let new_df = read_df(path, opts).expect(READ_ERROR);
            df = df
                .collect()
                .expect(COLLECT_ERROR)
                .vstack(&new_df)
                .unwrap()
                .lazy();
        }
    }

    if let Some(data) = &opts.add_row {
        df = add_row(df, data.to_owned(), &opts)?;
    }

    if let Some(col_vals) = opts.add_col_vals() {
        df = add_col_lazy(df, &col_vals.name, &col_vals.value, &col_vals.dtype)?;
    }

    if let Some(columns) = opts.columns_names() {
        if !columns.is_empty() {
            let cols = columns.iter().map(|c| col(c)).collect::<Vec<Expr>>();
            df = df.select(&cols);
        }
    }

    match opts.get_filter() {
        Ok(filter) => {
            if let Some(filter) = filter {
                df = filter_df(df, &filter)?;
            }
        }
        Err(err) => {
            log::error!("Could not parse filter: {err}");
        }
    }

    if let Some(columns) = opts.unique_columns() {
        df = unique_df(df, columns)?;
    }

    if let Some(agg) = &opts.get_aggregation()? {
        df = aggregate_df(df, agg)?;
    }

    if opts.should_randomize {
        let mut rand_indices: Vec<u32> = (0..height as u32).collect();
        rand_indices.shuffle(&mut thread_rng());
        df = take(df, rand_indices)?.lazy();
    }

    if let Some(sort_by) = &opts.sort_by {
        df = df.sort(sort_by, SortOptions::default());
    }

    if opts.should_reverse {
        df = df.reverse();
    }

    // These ops should be the last ops since they depends on order
    if let Some(indices) = opts.take_indices() {
        df = take(df, indices).unwrap().lazy();
    }

    // Maybe slice it up
    df = slice(df, &opts);
    df = head(df, &opts);
    df = tail(df, height, &opts);

    if let Some(item) = opts.column_at() {
        let full_df = df.collect().unwrap();
        let value = full_df.column(&item.col).unwrap().get(item.index).unwrap();
        let s1 = Series::new("", &[value]);
        let df = DataFrame::new(vec![s1]).unwrap();
        return Ok(df);
    }

    Ok(df.collect().expect(COLLECT_ERROR))
}

fn head(df: LazyFrame, opts: &DFOpts) -> LazyFrame {
    if let Some(head) = opts.head {
        df.slice(0, head as u32)
    } else {
        df
    }
}

fn tail(df: LazyFrame, height: usize, opts: &DFOpts) -> LazyFrame {
    if let Some(tail) = opts.tail {
        let start = (height - tail) as i64;
        let end = (height - 1) as u32;
        df.slice(start, end)
    } else {
        df
    }
}

fn slice(df: LazyFrame, opts: &DFOpts) -> LazyFrame {
    log::debug!("SLICE {:?}", opts.slice);
    if opts.page.is_some() || opts.page_size.is_some() {
        let page = opts.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
        let page_size = opts.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);
        let start = (page - 1) * page_size;
        df.slice(start as i64, page_size as u32)
    } else if let Some((start, end)) = opts.slice_indices() {
        log::debug!("SLICE with indices {:?}..{:?}", start, end);
        if start >= end {
            panic!("Slice error: Start must be greater than end.");
        }
        let len = end - start;
        df.slice(start, len as u32)
    } else {
        df
    }
}

pub fn df_add_row_num(df: DataFrame) -> Result<DataFrame, OxenError> {
    Ok(df
        .with_row_count(constants::ROW_NUM_COL_NAME, Some(0))
        .expect(COLLECT_ERROR))
}

pub fn df_add_row_num_starting_at(df: DataFrame, start: u32) -> Result<DataFrame, OxenError> {
    Ok(df
        .with_row_count(constants::ROW_NUM_COL_NAME, Some(start))
        .expect(COLLECT_ERROR))
}

pub fn any_val_to_bytes(value: &AnyValue) -> Vec<u8> {
    match value {
        AnyValue::Null => Vec::<u8>::new(),
        AnyValue::Int64(val) => val.to_le_bytes().to_vec(),
        AnyValue::Int32(val) => val.to_le_bytes().to_vec(),
        AnyValue::Int8(val) => val.to_le_bytes().to_vec(),
        AnyValue::Float32(val) => val.to_le_bytes().to_vec(),
        AnyValue::Float64(val) => val.to_le_bytes().to_vec(),
        AnyValue::Utf8(val) => val.as_bytes().to_vec(),
        // TODO: handle rows with lists...
        // AnyValue::List(val) => {
        //     match val.dtype() {
        //         DataType::Int32 => {},
        //         DataType::Float32 => {},
        //         DataType::Utf8 => {},
        //         DataType::UInt8 => {},
        //         x => panic!("unable to parse list with value: {} and type: {:?}", x, x.inner_dtype())
        //     }
        // },
        AnyValue::Datetime(val, TimeUnit::Milliseconds, _) => val.to_le_bytes().to_vec(),
        _ => Vec::<u8>::new(),
    }
}

pub fn df_hash_rows(df: DataFrame) -> Result<DataFrame, OxenError> {
    let num_rows = df.height() as i64;

    let mut col_names = vec![];
    let schema = df.schema();
    for field in schema.iter_fields() {
        col_names.push(col(field.name()));
    }
    // println!("Hashing: {:?}", col_names);
    // println!("{:?}", df);

    let df = df
        .lazy()
        .select([
            all(),
            as_struct(&col_names)
                .apply(
                    move |s| {
                        // log::debug!("s: {:?}", s);

                        let pb = ProgressBar::new(num_rows as u64);
                        // downcast to struct
                        let ca = s.struct_()?;
                        let out: Utf8Chunked = ca
                            .into_iter()
                            // .par_bridge() // not sure why this is breaking
                            .map(|row| {
                                // log::debug!("row: {:?}", row);
                                pb.inc(1);
                                let mut buffer: Vec<u8> = vec![];
                                for elem in row.iter() {
                                    // log::debug!("Got elem[{}] {}", i, elem);
                                    let mut elem: Vec<u8> = any_val_to_bytes(elem);
                                    // println!("Elem[{}] bytes {:?}", i, elem);
                                    buffer.append(&mut elem);
                                }
                                // println!("__DONE__ {:?}", buffer);
                                let result = hasher::hash_buffer(&buffer);
                                // let result = xxh3_64(&buffer);
                                // let result: u64 = 0;
                                // println!("__DONE__ {}", result);
                                Some(result)
                            })
                            .collect();

                        Ok(Some(out.into_series()))
                    },
                    GetOutput::from_type(polars::prelude::DataType::Utf8),
                )
                .alias(constants::ROW_HASH_COL_NAME),
        ])
        .collect()
        .unwrap();
    log::debug!("Hashed rows: {}", df);
    Ok(df)
}

fn sniff_db_csv_delimiter(path: impl AsRef<Path>, opts: &DFOpts) -> Result<u8, OxenError> {
    if let Some(delimiter) = &opts.delimiter {
        if delimiter.len() != 1 {
            return Err(OxenError::basic_str("Delimiter must be a single character"));
        }
        return Ok(delimiter.as_bytes()[0]);
    }

    match csv_sniffer::Sniffer::new().sniff_path(path) {
        Ok(metadata) => Ok(metadata.dialect.delimiter),
        Err(err) => {
            let err = format!("Error sniffing csv {:?}", err);
            log::warn!("{}", err);
            Ok(b',')
        }
    }
}

pub fn read_df<P: AsRef<Path>>(path: P, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(OxenError::file_does_not_exist(path));
    }

    let extension = path.extension().and_then(OsStr::to_str);
    let err = format!("Unknown file type read_df {path:?} -> {extension:?}");

    let df = match extension {
        Some(extension) => match extension {
            "ndjson" => read_df_jsonl(path),
            "jsonl" => read_df_jsonl(path),
            "json" => read_df_json(path),
            "csv" => {
                let delimiter = sniff_db_csv_delimiter(path, &opts)?;
                read_df_csv(path, delimiter)
            }
            "tsv" => read_df_csv(path, b'\t'),
            "parquet" => read_df_parquet(path),
            "arrow" => read_df_arrow(path),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }?;

    if opts.has_transform() {
        let df = transform(df, opts)?;
        Ok(df)
    } else {
        Ok(df)
    }
}

pub fn scan_df<P: AsRef<Path>>(path: P, opts: &DFOpts) -> Result<LazyFrame, OxenError> {
    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
    let err = format!("Unknown file type scan_df {input_path:?} {extension:?}");

    match extension {
        Some(extension) => match extension {
            "ndjson" => scan_df_jsonl(path),
            "jsonl" => scan_df_jsonl(path),
            "csv" => {
                let delimiter = sniff_db_csv_delimiter(&path, opts)?;
                scan_df_csv(path, delimiter)
            }
            "tsv" => scan_df_csv(path, b'\t'),
            "parquet" => scan_df_parquet(path),
            "arrow" => scan_df_arrow(path),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn write_df_json<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {output:?}");
    log::debug!("Writing file {:?}", output);
    log::debug!("{:?}", df);
    let f = std::fs::File::create(output).unwrap();
    JsonWriter::new(f)
        .with_json_format(JsonFormat::Json)
        .finish(df)
        .expect(&error_str);
    Ok(())
}

pub fn write_df_jsonl<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {output:?}");
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(output).unwrap();
    JsonWriter::new(f)
        .with_json_format(JsonFormat::JsonLines)
        .finish(df)
        .expect(&error_str);
    Ok(())
}

pub fn write_df_csv<P: AsRef<Path>>(
    df: &mut DataFrame,
    output: P,
    delimiter: u8,
) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {output:?}");
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(output).unwrap();
    CsvWriter::new(f)
        .has_header(true)
        .with_delimiter(delimiter)
        .finish(df)
        .expect(&error_str);
    Ok(())
}

pub fn write_df_parquet<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {output:?}");
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(output).unwrap();
    ParquetWriter::new(f).finish(df).expect(&error_str);
    Ok(())
}

pub fn write_df_arrow<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {output:?}");
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(output).unwrap();
    IpcWriter::new(f).finish(df).expect(&error_str);
    Ok(())
}

pub fn write_df<P: AsRef<Path>>(df: &mut DataFrame, path: P) -> Result<(), OxenError> {
    let path = path.as_ref();
    let extension = path.extension().and_then(OsStr::to_str);
    let err = format!("Unknown file type write_df {path:?} {extension:?}");

    match extension {
        Some(extension) => match extension {
            "ndjson" => write_df_jsonl(df, path),
            "jsonl" => write_df_jsonl(df, path),
            "json" => write_df_json(df, path),
            "tsv" => write_df_csv(df, path, b'\t'),
            "csv" => write_df_csv(df, path, b','),
            "parquet" => write_df_parquet(df, path),
            "arrow" => write_df_arrow(df, path),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn copy_df<P: AsRef<Path>>(input: P, output: P) -> Result<DataFrame, OxenError> {
    let mut df = read_df(input, DFOpts::empty())?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn copy_df_add_row_num<P: AsRef<Path>>(input: P, output: P) -> Result<DataFrame, OxenError> {
    let df = read_df(input, DFOpts::empty())?;
    let mut df = df
        .lazy()
        .with_row_count("_row_num", Some(0))
        .collect()
        .expect("Could not add row count");
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn show_path<P: AsRef<Path>>(input: P, opts: DFOpts) -> Result<DataFrame, OxenError> {
    let df = read_df(input, opts.clone())?;
    if opts.column_at().is_some() {
        for val in df.get(0).unwrap() {
            match val {
                polars::prelude::AnyValue::List(vals) => {
                    for val in vals.iter() {
                        println!("{val}")
                    }
                }
                _ => {
                    println!("{val}")
                }
            }
        }
    } else {
        println!("{df}");
    }

    Ok(df)
}

pub fn schema_to_string<P: AsRef<Path>>(
    input: P,
    flatten: bool,
    opts: &DFOpts,
) -> Result<String, OxenError> {
    let df = scan_df(input, opts)?;
    let schema = df.schema().expect("Could not get schema");

    if flatten {
        let mut result = String::new();
        for (i, field) in schema.iter_fields().enumerate() {
            if i != 0 {
                result = format!("{result},");
            }

            let dtype = DataType::from_polars(field.data_type());
            let field_str = String::from(field.name());
            let dtype_str = String::from(DataType::as_str(&dtype));
            result = format!("{result}{field_str}:{dtype_str}");
        }

        Ok(result)
    } else {
        let mut table = Table::new();
        table.set_header(vec!["column", "dtype"]);

        for field in schema.iter_fields() {
            let dtype = DataType::from_polars(field.data_type());
            let field_str = String::from(field.name());
            let dtype_str = String::from(DataType::as_str(&dtype));
            table.add_row(vec![field_str, dtype_str]);
        }

        Ok(format!("{table}"))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        df::{filter, tabular, DFOpts},
        error::OxenError,
    };
    use polars::prelude::*;

    #[test]
    fn test_filter_single_expr() -> Result<(), OxenError> {
        let query = Some("label == dog".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["cat", "dog", "unknown"],
            "min_x" => &["0.0", "1.0", "2.0"],
            "max_x" => &["3.0", "4.0", "5.0"],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        assert_eq!(
            r"shape: (1, 4)
┌──────────┬───────┬───────┬───────┐
│ image    ┆ label ┆ min_x ┆ max_x │
│ ---      ┆ ---   ┆ ---   ┆ ---   │
│ str      ┆ str   ┆ str   ┆ str   │
╞══════════╪═══════╪═══════╪═══════╡
│ 0001.jpg ┆ dog   ┆ 1.0   ┆ 4.0   │
└──────────┴───────┴───────┴───────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_filter_multiple_or_expr() -> Result<(), OxenError> {
        let query = Some("label == dog || label == cat".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["cat", "dog", "unknown"],
            "min_x" => &["0.0", "1.0", "2.0"],
            "max_x" => &["3.0", "4.0", "5.0"],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 4)
┌──────────┬───────┬───────┬───────┐
│ image    ┆ label ┆ min_x ┆ max_x │
│ ---      ┆ ---   ┆ ---   ┆ ---   │
│ str      ┆ str   ┆ str   ┆ str   │
╞══════════╪═══════╪═══════╪═══════╡
│ 0000.jpg ┆ cat   ┆ 0.0   ┆ 3.0   │
│ 0001.jpg ┆ dog   ┆ 1.0   ┆ 4.0   │
└──────────┴───────┴───────┴───────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_filter_multiple_and_expr() -> Result<(), OxenError> {
        let query = Some("label == dog && is_correct == true".to_string());
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "unknown"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let filter = filter::parse(query)?.unwrap();
        let filtered_df = tabular::filter_df(df.lazy(), &filter)?.collect().unwrap();

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (1, 5)
┌──────────┬───────┬───────┬───────┬────────────┐
│ image    ┆ label ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---   ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str   ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═══════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog   ┆ 0.0   ┆ 3.0   ┆ true       │
└──────────┴───────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_unique_single_field() -> Result<(), OxenError> {
        let fields = "label";
        let df = df!(
            "image" => &["0000.jpg", "0001.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "unknown"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let mut opts = DFOpts::from_unique(fields);
        // sort for tests because it comes back random
        opts.sort_by = Some(String::from("image"));
        let filtered_df = tabular::transform(df, opts)?;

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 5)
┌──────────┬─────────┬───────┬───────┬────────────┐
│ image    ┆ label   ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---     ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str     ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═════════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog     ┆ 0.0   ┆ 3.0   ┆ true       │
│ 0002.jpg ┆ unknown ┆ 2.0   ┆ 5.0   ┆ false      │
└──────────┴─────────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_unique_multi_field() -> Result<(), OxenError> {
        let fields = "image,label";
        let df = df!(
            "image" => &["0000.jpg", "0000.jpg", "0002.jpg"],
            "label" => &["dog", "dog", "dog"],
            "min_x" => &[0.0, 1.0, 2.0],
            "max_x" => &[3.0, 4.0, 5.0],
            "is_correct" => &[true, false, false],
        )
        .unwrap();

        let mut opts = DFOpts::from_unique(fields);
        // sort for tests because it comes back random
        opts.sort_by = Some(String::from("image"));
        let filtered_df = tabular::transform(df, opts)?;

        println!("{filtered_df}");

        assert_eq!(
            r"shape: (2, 5)
┌──────────┬───────┬───────┬───────┬────────────┐
│ image    ┆ label ┆ min_x ┆ max_x ┆ is_correct │
│ ---      ┆ ---   ┆ ---   ┆ ---   ┆ ---        │
│ str      ┆ str   ┆ f64   ┆ f64   ┆ bool       │
╞══════════╪═══════╪═══════╪═══════╪════════════╡
│ 0000.jpg ┆ dog   ┆ 0.0   ┆ 3.0   ┆ true       │
│ 0002.jpg ┆ dog   ┆ 2.0   ┆ 5.0   ┆ false      │
└──────────┴───────┴───────┴───────┴────────────┘",
            format!("{filtered_df}")
        );

        Ok(())
    }

    #[test]
    fn test_read_json() -> Result<(), OxenError> {
        let df = tabular::read_df_json("data/test/text/test.json")?;

        println!("{df}");

        assert_eq!(
            r"shape: (2, 3)
┌─────┬───────────┬──────────┐
│ id  ┆ text      ┆ category │
│ --- ┆ ---       ┆ ---      │
│ i64 ┆ str       ┆ str      │
╞═════╪═══════════╪══════════╡
│ 1   ┆ I love it ┆ positive │
│ 1   ┆ I hate it ┆ negative │
└─────┴───────────┴──────────┘",
            format!("{df}")
        );

        Ok(())
    }

    #[test]
    fn test_read_jsonl() -> Result<(), OxenError> {
        let df = tabular::read_df_jsonl("data/test/text/test.jsonl")?;

        println!("{df}");

        assert_eq!(
            r"shape: (2, 3)
┌─────┬───────────┬──────────┐
│ id  ┆ text      ┆ category │
│ --- ┆ ---       ┆ ---      │
│ i64 ┆ str       ┆ str      │
╞═════╪═══════════╪══════════╡
│ 1   ┆ I love it ┆ positive │
│ 1   ┆ I hate it ┆ negative │
└─────┴───────────┴──────────┘",
            format!("{df}")
        );

        Ok(())
    }
}
