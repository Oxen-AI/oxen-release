use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::ipc::writer::FileWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::{self, csv, json};
use datafusion::datasource::memory::MemTable;
use datafusion::prelude::{col, CsvReadOptions, DataFrame, ParquetReadOptions, SessionContext};

use colored::Colorize;
use comfy_table::modifiers::UTF8_ROUND_CORNERS;
use comfy_table::presets::UTF8_FULL;
use comfy_table::{Cell, ContentArrangement, Row, Table};

use termion::terminal_size;
use unicode_truncate::UnicodeTruncateStr;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::vec;

use crate::error::OxenError;
use crate::media::{tabular, DFOpts};
use crate::model::schema::Field;
use crate::model::Schema;
use crate::model::{CommitEntry, DataFrameDiff, LocalRepository};
use crate::util;

const MAX_CELL_LENGTH: usize = 128; // to truncate long text

async fn register_tsv_table(
    ctx: &SessionContext,
    path: &Path,
    name: &str,
) -> Result<(), OxenError> {
    log::debug!("Register TSV {:?}", path);
    let mut read_options = CsvReadOptions::new().schema_infer_max_records(100);
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
    let read_options = CsvReadOptions::new().schema_infer_max_records(100);
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

    let builder = json::ReaderBuilder::new().infer_schema(Some(1000));
    let file = File::open(path).unwrap();
    let mut reader = builder.build(file).unwrap();
    let mut batches: Vec<RecordBatch> = vec![];
    let mut batch_num: usize = 0;

    loop {
        match reader.next() {
            Ok(Some(read_batch)) => {
                batch_num += 1;
                log::debug!(
                    "Read batch {}, size {}x{}",
                    batch_num,
                    read_batch.num_rows(),
                    read_batch.num_columns()
                );
                batches.push(read_batch);
            }
            Err(e) => {
                panic!("{}", e);
            }
            Ok(None) => {
                break;
            }
        }
    }

    let provider = MemTable::try_new(reader.schema(), vec![batches])?;
    ctx.register_table(name, Arc::new(provider))?;

    log::debug!("Done register JSON {:?}", path);

    Ok(())
}

async fn register_parquet_table(
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
    } else if Some("csv") == extension {
        register_csv_table(ctx, path, name).await
    } else if Some("tsv") == extension {
        register_tsv_table(ctx, path, name).await
    } else if Some("parquet") == extension {
        register_parquet_table(ctx, path, name).await
    } else if Some("arrow") == extension {
        register_arrow_table(ctx, path, name).await
    } else {
        let err = format!("Unknown file type {:?}", extension);
        Err(OxenError::basic_str(err))
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
        run_all_query(ctx).await
    }
}

async fn run_all_query(ctx: &SessionContext) -> Result<Vec<RecordBatch>, OxenError> {
    run_query(ctx, "select * from data").await
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

pub fn write_batches_json<P: AsRef<Path>>(
    batches: &[RecordBatch],
    path: P,
) -> Result<(), OxenError> {
    let outpath = path.as_ref();
    println!("Writing JSON file {:?}", outpath);

    let file = File::create(outpath).unwrap();
    let mut writer = json::LineDelimitedWriter::new(file);
    writer.write_batches(batches).unwrap();

    Ok(())
}

pub fn write_batches_tsv<P: AsRef<Path>>(
    batches: &Vec<RecordBatch>,
    path: P,
) -> Result<(), OxenError> {
    // Write output to file to test
    let outpath = path.as_ref();
    println!("Writing TSV file {:?}", outpath);

    let file = File::create(outpath).unwrap();
    let builder = csv::WriterBuilder::new()
        .has_headers(true)
        .with_delimiter(b'\t');
    let mut writer = builder.build(file);

    let mut total_batches: usize = 0;
    for batch in batches {
        total_batches += 1;
        log::debug!("Writer wrote batch {}", total_batches);
        writer.write(batch).unwrap();
    }
    Ok(())
}

pub fn write_batches_csv<P: AsRef<Path>>(
    batches: &Vec<RecordBatch>,
    path: P,
) -> Result<(), OxenError> {
    // Write output to file to test
    let outpath = path.as_ref();
    println!("Writing CSV file {:?}", outpath);

    let file = File::create(outpath).unwrap();
    let builder = csv::WriterBuilder::new().has_headers(true);
    let mut writer = builder.build(file);

    let mut total_batches: usize = 0;
    for batch in batches {
        total_batches += 1;
        log::debug!("Writer wrote batch {}", total_batches);
        writer.write(batch).unwrap();
    }
    Ok(())
}

pub fn write_batches_parquet_with_size<P: AsRef<Path>>(
    batches: &Vec<RecordBatch>,
    path: P,
    size: usize,
) -> Result<(), OxenError> {
    let schema = batches[0].schema();
    let path = path.as_ref();
    let file = File::create(path)?;
    log::debug!("Writing parq file {:?}", path);

    // Default writer properties
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
        .set_write_batch_size(size)
        .build();

    let mut writer =
        datafusion::parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema, Some(props))
            .unwrap();

    let mut total_batches: usize = 0;
    for batch in batches {
        total_batches += 1;
        log::debug!("Writer wrote batch {}", total_batches);
        writer.write(batch).unwrap();
    }

    // writer must be closed to write footer
    writer.close().unwrap();

    Ok(())
}

pub fn write_batches_parquet<P: AsRef<Path>>(
    batches: &Vec<RecordBatch>,
    path: P,
) -> Result<(), OxenError> {
    let schema = batches[0].schema();
    let path = path.as_ref();
    let file = File::create(path)?;
    log::debug!("Writing parq file {:?}", path);

    // Default writer properties
    let props = datafusion::parquet::file::properties::WriterProperties::builder()
        .set_compression(datafusion::parquet::basic::Compression::SNAPPY)
        .build();

    let mut writer =
        datafusion::parquet::arrow::arrow_writer::ArrowWriter::try_new(file, schema, Some(props))
            .unwrap();

    let mut total_batches: usize = 0;
    for batch in batches {
        total_batches += 1;
        log::debug!("Writer wrote batch {}", total_batches);
        writer.write(batch).unwrap();
    }

    // writer must be closed to write footer
    writer.close().unwrap();

    Ok(())
}

pub fn write_batches_arrow<P: AsRef<Path>>(
    batches: &Vec<RecordBatch>,
    path: P,
) -> Result<(), OxenError> {
    let schema = batches[0].schema();

    // Write output to file to test
    let outpath = path.as_ref();
    println!("Writing arrow file {:?}", outpath);

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

pub fn write_batches<P: AsRef<Path>>(batches: &Vec<RecordBatch>, path: P) -> Result<(), OxenError> {
    if batches.is_empty() {
        eprintln!("Not writing empty data");
        return Ok(());
    }
    let path = path.as_ref();

    let extension = path.extension().and_then(OsStr::to_str);
    log::debug!("Got extension {:?}", extension);
    if extension == Some("ndjson") || Some("jsonl") == extension {
        write_batches_json(batches, path)
    } else if Some("tsv") == extension {
        write_batches_tsv(batches, path)
    } else if Some("csv") == extension {
        write_batches_csv(batches, path)
    } else if Some("parquet") == extension {
        write_batches_parquet(batches, path)
    } else if Some("arrow") == extension {
        write_batches_arrow(batches, path)
    } else {
        let err = format!("Unknown file type {:?}", extension);
        Err(OxenError::basic_str(err))
    }
}

/// TODO: Use polars instead of this hacky shit
/// This is hacky...but I don't understand datafusion well enough and need to get something going 🤦‍♂️
pub async fn group_rows_by_key<P: AsRef<Path>, S: AsRef<str>>(
    path: P,
    key: S,
) -> Result<
    (
        HashMap<String, Vec<Vec<String>>>,
        Arc<datafusion::arrow::datatypes::Schema>,
    ),
    OxenError,
> {
    let mut result: HashMap<String, Vec<Vec<String>>> = HashMap::new();

    let path = path.as_ref();
    let key = key.as_ref();
    let ctx = SessionContext::new();
    register_table(&ctx, path, "data").await?;
    let batches = run_all_query(&ctx).await?;

    if batches.is_empty() {
        let err = format!("Could not read data from {:?}", path);
        return Err(OxenError::basic_str(err));
    }

    let schema = batches[0].schema();
    let maybe_idx = schema.fields().iter().position(|x| x.name() == key);
    if maybe_idx.is_none() {
        let err = format!("Key not found: {key}");
        return Err(OxenError::basic_str(err));
    }

    // TODO: probably a more data-fusion-y way to aggregate and group
    let idx = maybe_idx.unwrap();
    for batch in batches {
        for row_i in 0..batch.num_rows() {
            // Get filename
            let filename =
                match arrow::util::display::array_value_to_string(batch.column(idx), row_i) {
                    Ok(filename) => {
                        if !result.contains_key(&filename) {
                            let new_rows: Vec<Vec<String>> = Vec::new();
                            result.insert(filename.clone(), new_rows);
                        }
                        filename
                    }
                    _ => {
                        log::error!("Invalid key value for column {idx}");
                        String::from("")
                    }
                };

            // Fill in rest of columns
            let mut row: Vec<String> = vec![];
            for col_i in 0..batch.num_columns() {
                match arrow::util::display::array_value_to_string(batch.column(col_i), row_i) {
                    Ok(val) => {
                        row.push(val);
                    }
                    _ => return Err(OxenError::basic_str("Invalid key value.")),
                }
            }
            result.get_mut(&filename).unwrap().push(row);
        }
    }

    Ok((result, schema))
}

/// TODO:
/// - Instead of converting to strings and grouping, use polars
/// - Move function to oxen annotate -a -n -f
/// - When you add a new row to the larger csv
///   - when saving....look at previous commit, combine, and uniq them
///   - test that we can add annotations from different files
///   - what do we do with different schemas from different csvs?
///     - gen a schema hash
///     - add to our list of schemas
///     - be able to name schema hashes?
///     - save schema's next to hashes
pub fn save_rows<P: AsRef<Path>>(
    path: P,
    rows: &[Vec<String>],
    schema: Arc<datafusion::arrow::datatypes::Schema>,
) -> Result<(), OxenError> {
    use std::io::Write;

    // Just writing a csv raw was way faster than using parquet 🤔
    // Since there probably won't be too many annotations per file...this seems fine for now
    let path = path.as_ref();

    let mut file = File::create(path)?;

    // Write header
    for (i, field) in schema.fields().iter().enumerate() {
        if i != 0 {
            file.write_all(b",")?;
        }
        file.write_all(field.name().as_bytes())?;
    }
    file.write_all(b"\n")?;

    // Write data
    for row in rows {
        for (i, col) in row.iter().enumerate() {
            if i != 0 {
                file.write_all(b",")?;
            }
            file.write_all(col.as_bytes())?;
        }
        file.write_all(b"\n")?;
    }

    Ok(())
}

pub async fn df_to_str(df: &Arc<DataFrame>) -> Result<String, OxenError> {
    let batches = df.collect().await?;
    if batches.is_empty() {
        return Ok(String::from("[]"));
    }

    // Keep it under max_table_width wide
    let schema = batches[0].schema();

    let max_table_width = terminal_size()?.0 - 20;
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
                                if val.len() > MAX_CELL_LENGTH {
                                    let (trunc, _size) = val.unicode_truncate(MAX_CELL_LENGTH);
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

    log::debug!("{total_result_rows} > {max_rows}");
    if total_result_rows > max_rows {
        let mut row = Row::new();
        row.add_cell(Cell::new("..."));
        table.add_row(row);
    }

    // Convert table to string
    Ok(format!(
        "\n{table}\n {} Rows x {} Columns",
        total_result_rows,
        schema.fields().len()
    ))
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
                                if val.len() > MAX_CELL_LENGTH {
                                    let (trunc, _size) = val.unicode_truncate(MAX_CELL_LENGTH);
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

fn get_added_schema_fields(schema_commit: &Schema, schema_current: &Schema) -> Vec<Field> {
    let mut fields: Vec<Field> = vec![];

    // if field is in current schema but not in commit, it was added
    for current_field in schema_current.fields.iter() {
        if !schema_commit
            .fields
            .iter()
            .any(|f| f.name == current_field.name)
        {
            fields.push(current_field.clone());
        }
    }

    fields
}

fn get_removed_schema_fields(schema_commit: &Schema, schema_current: &Schema) -> Vec<Field> {
    let mut fields: Vec<Field> = vec![];

    // if field is in commit history but not in current, it was removed
    for commit_field in schema_commit.fields.iter() {
        if !schema_current
            .fields
            .iter()
            .any(|f| f.name == commit_field.name)
        {
            fields.push(commit_field.clone());
        }
    }

    fields
}

pub async fn diff(repo: &LocalRepository, entry: &CommitEntry) -> Result<DataFrameDiff, OxenError> {
    let current_path = repo.path.join(&entry.path);
    let version_path = util::fs::version_path(repo, entry);

    log::debug!("DIFF current: {:?}", current_path);
    log::debug!("DIFF commit:  {:?}", version_path);

    let ctx = SessionContext::new();
    register_table(&ctx, &current_path, "current").await?;
    register_table(&ctx, &version_path, "commit").await?;

    let df_current = ctx.table("current")?;
    let df_commit = ctx.table("commit")?;

    // Hacky that we are using two different dataframe libraries here...but want to get this release out.
    let schema_commit = Schema::from_datafusion(df_commit.schema());
    let schema_current = Schema::from_datafusion(df_current.schema());
    if schema_commit.hash != schema_current.hash {
        let added_fields = get_added_schema_fields(&schema_commit, &schema_current);
        let removed_fields = get_removed_schema_fields(&schema_commit, &schema_current);

        if !added_fields.is_empty() {
            let opts = DFOpts::from_filter_fields(added_fields);
            let df_added = tabular::read_df(&current_path, opts)?;
            let added_str = format!("{}", df_added).green();
            println!("Added Cols\n{}\n", added_str);
        }

        if !removed_fields.is_empty() {
            let opts = DFOpts::from_filter_fields(removed_fields);
            let df_removed = tabular::read_df(&version_path, opts)?;
            let removed_str = format!("{}", df_removed).red();
            println!("Removed Cols\n{}", removed_str);
        }

        return Err(OxenError::schema_has_changed(schema_commit, schema_current));
    }

    // If we don't sort, it is non-deterministic the order the diff will come out
    // SORT ASC NULLS_FIRST
    let first_col = df_commit.schema().field(0);
    let df_commit = ctx.table("commit")?;
    let diff_added = df_current
        .except(df_commit)?
        .sort(vec![col(first_col.name()).sort(true, true)])?;

    let df_commit = ctx.table("commit")?;
    let diff_removed = df_commit
        .except(df_current)?
        .sort(vec![col(first_col.name()).sort(true, true)])?;

    Ok(DataFrameDiff {
        added: diff_added,
        removed: diff_removed,
    })
}

#[cfg(test)]
mod tests {
    use crate::command;
    use crate::error::OxenError;
    use crate::index::CommitDirReader;
    use crate::media::tabular_datafusion;
    use crate::test;
    use crate::util;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_tabular_group_rows_by_key() -> Result<(), OxenError> {
        test::run_empty_dir_test_async(|dir| async move {
            // Header
            let mut annotations_content = String::from("file,x,y\n");
            // Annotations, two on first file, one on other two
            annotations_content.push_str("img_1.txt,199,223\n");
            annotations_content.push_str("img_1.txt,234,432\n");
            annotations_content.push_str("img_2.txt,121,221\n");
            annotations_content.push_str("img_3.txt,324,543\n");
            let annotation_file = test::add_csv_file_to_dir(&dir, &annotations_content)?;

            let (results, _schema) =
                tabular_datafusion::group_rows_by_key(&annotation_file, "file").await?;
            assert!(results.get("img_1.txt").is_some());
            assert_eq!(results.get("img_1.txt").unwrap().len(), 2);
            assert_eq!(results.get("img_1.txt").unwrap()[0][0], "img_1.txt");
            assert_eq!(results.get("img_1.txt").unwrap()[0][1], "199");
            assert_eq!(results.get("img_1.txt").unwrap()[0][2], "223");

            Ok(dir)
        })
        .await
    }

    #[test]
    fn test_tabular_save_rows() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            let file = dir.join("my.csv");
            let schema = Arc::new(Schema::new(vec![
                Field::new("file", DataType::Utf8, false),
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Int32, false),
            ]));
            let rows: Vec<Vec<String>> = vec![
                vec![
                    String::from("img_1.txt"),
                    String::from("199"),
                    String::from("223"),
                ],
                vec![
                    String::from("img_1.txt"),
                    String::from("200"),
                    String::from("224"),
                ],
                vec![
                    String::from("img_2.txt"),
                    String::from("201"),
                    String::from("225"),
                ],
            ];
            tabular_datafusion::save_rows(&file, &rows, schema)?;

            let data = util::fs::read_from_path(&file)?;
            assert_eq!(
                data,
                r"file,x,y
img_1.txt,199,223
img_1.txt,200,224
img_2.txt,201,225
"
            );

            Ok(())
        })
    }

    #[tokio::test]
    async fn test_tabular_diff_added() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();
            let commit_entry_reader = CommitDirReader::new(&repo, last_commit)?;

            let bbox_file = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file =
                test::append_line_txt_file(bbox_file, "train/cat_3.jpg,41.0,31.5,410,427")?;

            let relative = util::fs::path_relative_to_dir(&bbox_file, &repo.path)?;
            let entry = commit_entry_reader.get_entry(&relative)?.unwrap();
            let diff = tabular_datafusion::diff(&repo, &entry).await?;
            let results = r"
╭─────────────────┬───────┬───────┬───────┬────────╮
│ file            ┆ min_x ┆ min_y ┆ width ┆ height │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/cat_3.jpg ┆ 41    ┆ 31.5  ┆ 410   ┆ 427    │
╰─────────────────┴───────┴───────┴───────┴────────╯
 1 Rows x 5 Columns";

            assert_eq!(results, tabular_datafusion::df_to_str(&diff.added).await?);
            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn test_tabular_diff_removed() -> Result<(), OxenError> {
        test::run_training_data_repo_test_fully_committed_async(|repo| async move {
            let commits = command::log(&repo)?;
            let last_commit = commits.first().unwrap();
            let commit_entry_reader = CommitDirReader::new(&repo, last_commit)?;

            let bbox_file = repo
                .path
                .join("annotations")
                .join("train")
                .join("bounding_box.csv");
            let bbox_file = test::modify_txt_file(
                bbox_file,
                r"
file,min_x,min_y,width,height
train/dog_1.jpg,101.5,32.0,385,330
train/dog_2.jpg,7.0,29.5,246,247
train/cat_2.jpg,30.5,44.0,333,396
",
            )?;

            let relative = util::fs::path_relative_to_dir(&bbox_file, &repo.path)?;
            let entry = commit_entry_reader.get_entry(&relative)?.unwrap();
            let diff = tabular_datafusion::diff(&repo, &entry).await?;
            let results = r"
╭─────────────────┬───────┬───────┬───────┬────────╮
│ file            ┆ min_x ┆ min_y ┆ width ┆ height │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/cat_1.jpg ┆ 57    ┆ 35.5  ┆ 304   ┆ 427    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ train/dog_3.jpg ┆ 19    ┆ 63.5  ┆ 376   ┆ 421    │
╰─────────────────┴───────┴───────┴───────┴────────╯
 2 Rows x 5 Columns";

            assert_eq!(results, tabular_datafusion::df_to_str(&diff.removed).await?);
            Ok(())
        })
        .await
    }
}
