use polars::prelude::*;

use crate::error::OxenError;

use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;

pub fn read_df_csv<P: AsRef<Path>>(path: P, delimiter: u8) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read csv from path".to_string();
    let path = path.as_ref();
    let df = CsvReader::from_path(path)
        .expect(&error_str)
        .infer_schema(Some(100))
        .has_header(true)
        .with_delimiter(delimiter)
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn read_df_json<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read tabular data from path".to_string();
    let file = File::open(path.as_ref())?;
    let df = JsonReader::new(file)
        .infer_schema_len(Some(100))
        .finish()
        .expect(&error_str);
    Ok(df)
}

pub fn read_df_parquet<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read tabular data from path".to_string();
    let file = File::open(path.as_ref())?;
    let df = ParquetReader::new(file).finish().expect(&error_str);
    Ok(df)
}

pub fn read_df_arrow<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let error_str = "Could not read tabular data from path".to_string();
    let file = File::open(path.as_ref())?;
    let df = IpcReader::new(file).finish().expect(&error_str);
    Ok(df)
}

pub fn read_df<P: AsRef<Path>>(path: P) -> Result<DataFrame, OxenError> {
    let input_path = path.as_ref();
    let extension = input_path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    let err = format!("Unknown file type {:?}", extension);

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
    let mut df = read_df(input)?;
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn copy_df_add_row_num<P: AsRef<Path>>(input: P, output: P) -> Result<DataFrame, OxenError> {
    let df = read_df(input)?;
    let mut df = df
        .lazy()
        .with_row_count("_row_num", Some(0))
        .collect()
        .expect("Could not add row count");
    write_df_arrow(&mut df, output)?;
    Ok(df)
}

pub fn show_path<P: AsRef<Path>>(input: P) -> Result<DataFrame, OxenError> {
    let df = read_df(input)?;
    println!("{}", df);
    Ok(df)
}
