use polars::prelude::*;

use crate::constants;
use crate::error::OxenError;
use crate::media::df_opts::DFOpts;
use crate::model::schema::DataType;
use crate::util::hasher;

use indicatif::ProgressBar;
use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;

use super::df_opts::{DFFilter, DFFilterOp};

const DEFAULT_INFER_SCHEMA_LEN: usize = 100;
const READ_ERROR: &str = "Could not read tabular data from path";

pub fn read_df_csv<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read csv from path".to_string();
    let df = CsvReader::from_path(path.as_ref())
        .expect(&error_str)
        .infer_schema(Some(DEFAULT_INFER_SCHEMA_LEN))
        .has_header(true)
        .with_delimiter(delimiter)
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn scan_df_csv<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<LazyFrame, OxenError> {
    Ok(LazyCsvReader::new(path)
        .with_delimiter(delimiter)
        .with_infer_schema_length(Some(DEFAULT_INFER_SCHEMA_LEN))
        .has_header(true)
        .finish()
        .expect(READ_ERROR))
}

pub fn read_df_json<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read tabular data from path".to_string();
    let file = File::open(path.as_ref())?;
    let df = JsonReader::new(file)
        .infer_schema_len(Some(DEFAULT_INFER_SCHEMA_LEN))
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn scan_df_json<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyJsonLineReader::new(
        path.as_ref()
            .to_str()
            .expect("Invalid json path.")
            .to_string(),
    )
    .with_infer_schema_length(Some(DEFAULT_INFER_SCHEMA_LEN))
    .finish()
    .expect(READ_ERROR))
}

pub fn read_df_parquet<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read tabular data from path".to_string();
    let file = File::open(path.as_ref())?;
    let df = ParquetReader::new(file).finish().expect(&error_str);
    Ok(df)
}

pub fn scan_df_parquet<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyFrame::scan_parquet(path, ScanArgsParquet::default()).expect(READ_ERROR))
}

fn read_df_arrow<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let file = File::open(path.as_ref())?;
    Ok(IpcReader::new(file).finish().expect(READ_ERROR))
}

fn scan_df_arrow<P: AsRef<Path>>(path: P) -> Result<LazyFrame, OxenError> {
    Ok(LazyFrame::scan_ipc(path, ScanArgsIpc::default()).expect(READ_ERROR))
}

pub fn take(df: LazyFrame, indices: Vec<u32>) -> Result<DataFrame, OxenError> {
    let idx = IdxCa::new("idx", &indices);
    Ok(df
        .collect()
        .expect(READ_ERROR)
        .take(&idx)
        .expect(READ_ERROR))
}

pub fn add_col(df: LazyFrame, name: &str, val: &str, dtype: &str) -> Result<LazyFrame, OxenError> {
    let mut df = df.collect().expect(READ_ERROR);

    let dtype = DataType::from_string(dtype).to_polars();

    let column = Series::new_empty(name, &dtype);
    let column = column
        .extend_constant(AnyValue::Utf8(val), df.height())
        .expect("Could not extend df");
    df.with_column(column).expect(READ_ERROR);
    let df = df.lazy();
    Ok(df)
}

pub fn add_row(df: LazyFrame, vals: Vec<String>) -> Result<LazyFrame, OxenError> {
    let df = df.collect().expect(READ_ERROR);

    if df.width() != vals.len() {
        let err = format!(
            "Cannot add row of len {} to data frame of width {}",
            vals.len(),
            df.width()
        );
        return Err(OxenError::basic_str(err));
    }

    let mut series: Vec<Series> = vec![];
    for (i, field) in df.fields().iter().enumerate() {
        let s: Series = Series::from_any_values(
            &field.name,
            &[val_from_str_and_dtype(&vals[i], field.data_type())],
        )
        .expect("Could not create col from row val");
        series.push(s);
    }

    let new_row = DataFrame::new(series).unwrap();
    let df = df.vstack(&new_row).unwrap().lazy();
    Ok(df)
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
        _ => panic!("Currently do not support data type"),
    }
}

fn val_from_df_and_filter<'a>(df: &'a LazyFrame, filter: &'a DFFilter) -> AnyValue<'a> {
    if let Some(value) = df
        .schema()
        .unwrap()
        .iter_fields()
        .find(|f| f.name == filter.field)
    {
        val_from_str_and_dtype(&filter.value, value.data_type())
    } else {
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
        _ => panic!("Unknown data type to create literal"),
    }
}

fn filter_df(df: LazyFrame, filter: &DFFilter) -> Result<LazyFrame, OxenError> {
    let val = val_from_df_and_filter(&df, filter);
    let val = lit_from_any(&val);
    match filter.op {
        DFFilterOp::EQ => Ok(df.filter(col(&filter.field).eq(val))),
        DFFilterOp::GT => Ok(df.filter(col(&filter.field).gt(val))),
        DFFilterOp::LT => Ok(df.filter(col(&filter.field).lt(val))),
        DFFilterOp::GTE => Ok(df.filter(col(&filter.field).gt_eq(val))),
        DFFilterOp::LTE => Ok(df.filter(col(&filter.field).lt_eq(val))),
        DFFilterOp::NEQ => Ok(df.filter(col(&filter.field).neq(val))),
    }
}

pub fn transform_df(mut df: LazyFrame, opts: &DFOpts) -> Result<DataFrame, OxenError> {
    log::debug!("Got filter ops {:?}", opts);

    if let Some(row_vals) = opts.add_row_vals() {
        df = add_row(df, row_vals)?;
    }

    if let Some(col_vals) = opts.add_col_vals() {
        df = add_col(df, &col_vals.name, &col_vals.value, &col_vals.dtype)?;
    }

    if let Some(filter) = opts.get_filter() {
        df = filter_df(df, &filter)?;
    }

    if let Some(columns) = opts.columns_names() {
        if !columns.is_empty() {
            let cols = columns.iter().map(|c| col(c)).collect::<Vec<Expr>>();
            df = df.select(&cols);
        }
    }

    if let Some((offset, len)) = opts.slice_indices() {
        return Ok(df.slice(offset, len as u32).collect().expect(READ_ERROR));
    }

    if let Some(indices) = opts.take_indices() {
        return take(df, indices);
    }

    Ok(df.collect().expect(READ_ERROR))
}

pub fn df_add_row_num(df: DataFrame) -> Result<DataFrame, OxenError> {
    Ok(df
        .with_row_count(constants::ROW_NUM_COL_NAME, Some(0))
        .expect(READ_ERROR))
}

pub fn df_add_row_num_starting_at(df: DataFrame, start: u32) -> Result<DataFrame, OxenError> {
    Ok(df
        .with_row_count(constants::ROW_NUM_COL_NAME, Some(start))
        .expect(READ_ERROR))
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
                            .map(|row| {
                                // log::debug!("row: {:?}", row);
                                pb.inc(1);
                                let mut buffer: Vec<u8> = vec![];
                                for elem in row.iter() {
                                    // log::debug!("Got elem[{}] {}", i, elem);
                                    let mut elem: Vec<u8> = match elem {
                                        AnyValue::Null => Vec::<u8>::new(),
                                        AnyValue::Int64(val) => val.to_le_bytes().to_vec(),
                                        AnyValue::Int32(val) => val.to_le_bytes().to_vec(),
                                        AnyValue::Int8(val) => val.to_le_bytes().to_vec(),
                                        AnyValue::Float32(val) => val.to_le_bytes().to_vec(),
                                        AnyValue::Float64(val) => val.to_le_bytes().to_vec(),
                                        AnyValue::Utf8(val) => val.as_bytes().to_vec(),
                                        // AnyValue::List(val) => {
                                        //     match val.dtype() {
                                        //         DataType::Int32 => {},
                                        //         DataType::Float32 => {},
                                        //         DataType::Utf8 => {},
                                        //         DataType::UInt8 => {},
                                        //         x => panic!("unable to parse list with value: {} and type: {:?}", x, x.inner_dtype())
                                        //     }
                                        // },
                                        AnyValue::Datetime(val, TimeUnit::Milliseconds, _) => {
                                            val.to_le_bytes().to_vec()
                                        }
                                        _ => Vec::<u8>::new(),
                                    };
                                    // println!("Elem[{}] bytes {:?}", i, elem);
                                    buffer.append(&mut elem);
                                }
                                // println!("__DONE__ {:?}", buffer);
                                let result = hasher::hash_buffer(&buffer);
                                // println!("__DONE__ {}", result);
                                Some(result)
                            })
                            .collect();

                        Ok(out.into_series())
                    },
                    GetOutput::from_type(polars::prelude::DataType::UInt64),
                )
                .alias(constants::ROW_HASH_COL_NAME),
        ])
        .collect()
        .unwrap();
    log::debug!("Hashed rows: {}", df);
    Ok(df)
}

pub fn read_df<P: AsRef<Path>>(path: P, opts: &DFOpts) -> Result<DataFrame, OxenError> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(OxenError::file_does_not_exist(path));
    }

    let extension = path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    let err = format!("Unknown file type {:?}", extension);

    if opts.has_transform() {
        let df = scan_df(path, opts)?;
        let df = transform_df(df, opts)?;
        Ok(df)
    } else {
        match extension {
            Some(extension) => match extension {
                "ndjson" => read_df_json(path),
                "jsonl" => read_df_json(path),
                "tsv" => read_df_csv(path, b'\t'),
                "csv" => read_df_csv(path, b','),
                "parquet" => read_df_parquet(path),
                "arrow" => read_df_arrow(path),
                _ => Err(OxenError::basic_str(err)),
            },
            None => Err(OxenError::basic_str(err)),
        }
    }
}

pub fn scan_df<P: AsRef<Path>>(path: P, _opts: &DFOpts) -> Result<LazyFrame, OxenError> {
    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    let err = format!("Unknown file type {:?}", extension);

    match extension {
        Some(extension) => match extension {
            "ndjson" => scan_df_json(path),
            "jsonl" => scan_df_json(path),
            "tsv" => scan_df_csv(path, b'\t'),
            "csv" => scan_df_csv(path, b','),
            "parquet" => scan_df_parquet(path),
            "arrow" => scan_df_arrow(path),
            _ => Err(OxenError::basic_str(err)),
        },
        None => Err(OxenError::basic_str(err)),
    }
}

pub fn write_df_json<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {:?}", output);
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(&output).unwrap();
    JsonWriter::new(f).finish(df).expect(&error_str);
    Ok(())
}

pub fn write_df_csv<P: AsRef<Path>>(
    df: &mut DataFrame,
    output: P,
    delimiter: u8,
) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {:?}", output);
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(&output).unwrap();
    CsvWriter::new(f)
        .has_header(true)
        .with_delimiter(delimiter)
        .finish(df)
        .expect(&error_str);
    Ok(())
}

pub fn write_df_parquet<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {:?}", output);
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(&output).unwrap();
    ParquetWriter::new(f).finish(df).expect(&error_str);
    Ok(())
}

pub fn write_df_arrow<P: AsRef<Path>>(df: &mut DataFrame, output: P) -> Result<(), OxenError> {
    let output = output.as_ref();
    let error_str = format!("Could not save tabular data to path: {:?}", output);
    log::debug!("Writing file {:?}", output);
    let f = std::fs::File::create(&output).unwrap();
    IpcWriter::new(f).finish(df).expect(&error_str);
    Ok(())
}

pub fn write_df<P: AsRef<Path>>(df: &mut DataFrame, path: P) -> Result<(), OxenError> {
    let path = path.as_ref();
    let extension = path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    let err = format!("Unknown file type {:?}", extension);

    match extension {
        Some(extension) => match extension {
            "ndjson" => write_df_json(df, path),
            "jsonl" => write_df_json(df, path),
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
    let opts = DFOpts::empty();
    let mut df = read_df(input, &opts)?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn copy_df_add_row_num<P: AsRef<Path>>(input: P, output: P) -> Result<DataFrame, OxenError> {
    let opts = DFOpts::empty();
    let df = read_df(input, &opts)?;
    let mut df = df
        .lazy()
        .with_row_count("_row_num", Some(0))
        .collect()
        .expect("Could not add row count");
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn show_path<P: AsRef<Path>>(input: P, opts: &DFOpts) -> Result<DataFrame, OxenError> {
    let df = read_df(input, opts)?;
    println!("{}", df);
    Ok(df)
}
