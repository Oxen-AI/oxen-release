use datafusion::arrow;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::*;

use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Row, Table};

use termion::terminal_size;
use unicode_truncate::UnicodeTruncateStr;

use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use crate::error::OxenError;

async fn register_tsv_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register TSV {:?}", path);
    let mut read_options = CsvReadOptions::new();
    read_options.delimiter = b'\t';
    ctx.register_csv(name, path.to_str().unwrap(), read_options)
        .await?;
    log::debug!("Done register TSV {:?}", path);

    Ok(())
}

async fn register_csv_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register CSV {:?}", path);
    let read_options = CsvReadOptions::new();
    ctx.register_csv(name, path.to_str().unwrap(), read_options)
        .await?;
    log::debug!("Done register CSV {:?}", path);

    Ok(())
}

async fn register_json_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register JSON {:?}", path);
    let read_options = NdJsonReadOptions::default();
    ctx.register_json(name, path.to_str().unwrap(), read_options)
        .await?;
    log::debug!("Done register JSON {:?}", path);

    Ok(())
}

async fn register_parq_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register Parquet {:?}", path);
    let read_options = ParquetReadOptions::default();
    ctx.register_parquet(name, path.to_str().unwrap(), read_options)
        .await?;
    log::debug!("Done register Parquet {:?}", path);

    Ok(())
}

async fn register_arrow_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register Arrow {:?}", path);

    let file = File::open(path)?;
    let mut reader = FileReader::try_new(file, None).unwrap();
    println!("Reader read: {} batches", reader.num_batches());
    println!("Got schema: {:?}", reader.schema());

    let mut batches: Vec<RecordBatch> = vec![];
    let mut batch_num: usize = 0;

    loop {
        match reader.next() {
            Some(Ok(read_batch)) => {
                batch_num += 1;
                log::debug!(
                    "Read batch {}, size {}x{}",
                    batch_num,
                    read_batch.num_rows(),
                    read_batch.num_columns()
                );
                batches.push(read_batch);
            }
            Some(Err(e)) => {
                panic!("{}", e);
            }
            None => {
                break;
            }
        }
    }

    let provider = MemTable::try_new(reader.schema(), vec![batches])?;
    ctx.register_table(name, Arc::new(provider))?;

    log::debug!("Done register Arrow {:?}", path);

    Ok(())
}

async fn register_table(ctx: &SessionContext, path: &Path, name: &str) -> Result<(), OxenError> {
    let extension = path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    if extension == Some("ndjson") || Some("jsonl") == extension {
        register_json_table(ctx, path, name).await
    } else if Some("tsv") == extension {
        register_tsv_table(ctx, path, name).await
    } else if Some("parq") == extension || Some("parquet") == extension {
        register_parq_table(ctx, path, name).await
    } else if Some("arrow") == extension {
        register_arrow_table(ctx, path, name).await
    } else {
        register_csv_table(ctx, path, name).await
    }
}

async fn run_query_or_all<S: AsRef<str>>(
    ctx: &SessionContext,
    query: Option<S>,
) -> Result<Vec<RecordBatch>, OxenError> {
    if let Some(query) = query {
        let q = query.as_ref();
        run_query(ctx, q).await
    } else {
        run_query(ctx, "select * from data").await
    }
}

pub async fn transform_table<P: AsRef<Path>, S: AsRef<str>>(
    input: P,
    query: Option<S>,
    output: Option<P>,
) -> Result<(), OxenError> {
    let path = input.as_ref();
    let ctx = SessionContext::new();
    register_table(&ctx, path, "data").await?;

    let batches = run_query_or_all(&ctx, query).await?;
    print_batches(&ctx, &batches).await?;

    if let Some(path) = output {
        write_batches(&batches, path).unwrap();
    }

    Ok(())
}

pub async fn query_ctx(ctx: &SessionContext, query: &str) -> Result<Vec<RecordBatch>, OxenError> {
    let df = ctx.sql(query).await?;
    let results = df.collect().await?;
    Ok(results)
}

/// TODO:
/// - Write to different formats (parq, csv)
/// - Downcase all the schema values so we can query the output
pub fn write_batches<P: AsRef<Path>>(batches: &Vec<RecordBatch>, path: P) -> Result<(), OxenError> {
    if batches.is_empty() {
        eprintln!("Not writing empty data");
        return Ok(());
    }

    // Keep it under max_table_width wide
    let schema = batches[0].schema();

    // Write output to file to test
    let outpath = path.as_ref();
    let mut file = File::create(outpath).unwrap();
    let mut writer = FileWriter::try_new(&mut file, &schema).unwrap();

    let mut total_batches: usize = 0;
    for batch in batches {
        total_batches += 1;
        log::debug!("Writer wrote batch {}", total_batches);
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();

    Ok(())
}

pub async fn print_batches(
    ctx: &SessionContext,
    batches: &Vec<RecordBatch>,
) -> Result<(), OxenError> {
    log::debug!("Counting....");
    let count_df = ctx
        .sql("select count(*) from data")
        .await?
        .collect()
        .await?;
    let total_data_rows = count_df
        .first()
        .unwrap()
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    // let val = arrow::util::display::array_value_to_string(count_df.first().unwrap().column(0), 0).unwrap();
    let total_data_rows: usize = total_data_rows as usize;
    log::debug!("Got count {}", total_data_rows);

    if batches.is_empty() {
        println!("[]");
        return Ok(());
    }

    // Keep it under max_table_width wide
    let schema = batches[0].schema();

    let max_table_width = terminal_size()?.0 - 20;
    let max_cell_length = 256; // to truncate long text
    log::debug!("Max width: {max_table_width}");
    let max_cols: usize = 8;
    let max_rows: usize = 10;

    // Pretty print table
    let mut table = Table::new();
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic)
        .set_width(max_table_width);

    // Add header row
    let mut row = Row::new();
    for field in schema.fields() {
        row.add_cell(Cell::new(field.name()));
        if row.cell_count() > max_cols {
            row.add_cell(Cell::new("..."));
            break;
        }
    }
    table.add_row(row);

    let mut total_result_rows: usize = 0;
    for batch in batches {
        for row_i in 0..batch.num_rows() {
            if (total_result_rows + row_i) < max_rows {
                let mut row = Row::new();
                for col_i in 0..batch.num_columns() {
                    if col_i <= max_cols {
                        match arrow::util::display::array_value_to_string(
                            batch.column(col_i),
                            row_i,
                        ) {
                            Ok(mut val) => {
                                if val.len() > max_cell_length {
                                    let (trunc, _size) = val.unicode_truncate(max_cell_length);
                                    val = format!("{}...", trunc);
                                }
                                row.add_cell(Cell::new(&val));
                            }
                            _ => {
                                row.add_cell(Cell::new("?"));
                            }
                        }
                    }
                }
                table.add_row(row);
            }
        }
        total_result_rows += batch.num_rows();
    }

    log::debug!("{total_data_rows} > {max_rows}");
    if total_data_rows > max_rows {
        let mut row = Row::new();
        row.add_cell(Cell::new("..."));
        table.add_row(row);
    }

    // Print the table to stdout
    println!("{table}");
    println!(
        "{} Rows x {} Columns",
        total_data_rows,
        schema.fields().len()
    );

    Ok(())
}

pub async fn run_query(ctx: &SessionContext, query: &str) -> Result<Vec<RecordBatch>, OxenError> {
    log::debug!("Running query: `{}`", query);

    // limit N, START
    // "select * from data limit 3, 161290"
    // let query = format!("select * from data limit 3, 161290");

    let df = ctx.sql(query).await?;

    let results = df.collect().await?;
    Ok(results)
}
