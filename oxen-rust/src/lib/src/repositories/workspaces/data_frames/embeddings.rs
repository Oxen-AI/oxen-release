use arrow::array::FixedSizeListArray;
use arrow::array::{Float32Array, Float64Array, ListArray, RecordBatch};
use polars::frame::DataFrame;

use crate::constants::TABLE_NAME;
use crate::core::db::data_frames::df_db;
use crate::error::OxenError;
use crate::model::data_frame::schema::Field;
use crate::model::Workspace;
use crate::opts::EmbeddingQueryOpts;
use crate::repositories;

use std::path::Path;
use std::path::PathBuf;

fn vector_length_path(workspace: &Workspace, path: impl AsRef<Path>) -> PathBuf {
    let path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    let parent = path.parent().unwrap();
    parent.join("vector_length")
}

pub fn index(
    workspace: &Workspace,
    path: impl AsRef<Path>,
    column: impl AsRef<str>,
) -> Result<(), OxenError> {
    let path = path.as_ref();
    let column = column.as_ref();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, path);
    log::debug!("Embedding index DB Path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    // Make sure the existing column is a float vector
    let sql = format!("SELECT {} FROM df LIMIT 1;", column);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
    let Some(item) = result_set.first() else {
        return Err(OxenError::basic_str("No items found"));
    };
    let first_column = item.column(0);
    log::debug!("First column: {:?}", first_column);

    // Check if the column is a list of floats/doubles
    let vector_length = match first_column.data_type() {
        arrow::datatypes::DataType::List(field) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float32Array>() {
                    first_value.len()
                } else {
                    return Err(OxenError::basic_str(
                        "Expected Float32Array inside ListArray",
                    ));
                }
            }
            arrow::datatypes::DataType::Float64 => {
                let array = first_column
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                if let Some(first_value) = array.value(0).as_any().downcast_ref::<Float64Array>() {
                    first_value.len()
                } else {
                    return Err(OxenError::basic_str(
                        "Expected Float64Array inside ListArray",
                    ));
                }
            }
            _ => {
                return Err(OxenError::basic_str(
                    "Column must be a list of float32 or float64",
                ))
            }
        },
        arrow::datatypes::DataType::FixedSizeList(field, size) => match field.data_type() {
            arrow::datatypes::DataType::Float32 => *size as usize,
            _ => {
                return Err(OxenError::basic_str(
                    "Column FixedSizeList must be a float32 type",
                ))
            }
        },
        _ => return Err(OxenError::basic_str("Column must be a list type")),
    };

    log::debug!("Vector length: {}", vector_length);
    // Write the vector length to a file we can use in the query
    let vector_length_path = vector_length_path(workspace, path);
    std::fs::write(vector_length_path, vector_length.to_string())?;

    // Execute VSS commands separately
    conn.execute("INSTALL vss;", [])?;
    conn.execute("LOAD vss;", [])?;
    conn.execute("SET hnsw_enable_experimental_persistence = true;", [])?;

    // must convert float64 to float32 for the vector search to work
    let sql = format!(
        "ALTER TABLE df ALTER COLUMN {} TYPE FLOAT[{}];",
        column, vector_length
    );
    log::debug!("Executing: {}", sql);
    conn.execute(&sql, [])?;

    Ok(())
}

pub fn query(workspace: &Workspace, opts: &EmbeddingQueryOpts) -> Result<DataFrame, OxenError> {
    let (key, value) = opts.parse_query()?;
    let column = opts.column.clone();
    let path = opts.path.clone();
    let similarity_column = opts.name.clone();

    let db_path = repositories::workspaces::data_frames::duckdb_path(workspace, &path);
    log::debug!("Embedding query DB Path: {:?}", db_path);
    let conn = df_db::get_connection(&db_path)?;

    let sql = format!("SELECT {} FROM df WHERE {} = '{}';", column, key, value);
    log::debug!("Executing: {}", sql);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();
    log::debug!("Result set: {:?}", result_set);

    // Read the vector length from the file we wrote in the index function
    let vector_length_path = vector_length_path(workspace, path);
    let vector_length = std::fs::read_to_string(vector_length_path)?.parse::<usize>()?;
    log::debug!("Vector length: {}", vector_length);
    // Average the embeddings
    let avg_embedding = get_avg_embedding(result_set)?;
    log::debug!("Avg embedding: {:?}", avg_embedding);
    let embedding_str = format!(
        "[{}]",
        avg_embedding
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
            .join(",")
    );
    let sql = format!("SELECT *, array_cosine_similarity({column}, {embedding_str}::FLOAT[{vector_length}]) as {similarity_column} FROM df ORDER BY {similarity_column} DESC");
    log::debug!("Executing: {}", sql);
    let result_set: Vec<RecordBatch> = conn.prepare(&sql)?.query_arrow([])?.collect();

    let mut schema = df_db::get_schema(&conn, TABLE_NAME)?;
    schema.fields.push(Field::new(&similarity_column, "f32"));
    let df = df_db::record_batches_to_polars_df_explicit_nulls(result_set, &schema)?;
    Ok(df)
}

fn get_avg_embedding(result_set: Vec<RecordBatch>) -> Result<Vec<f32>, OxenError> {
    let mut embeddings: Vec<Vec<f32>> = Vec::new();
    let mut vector_length = 0;
    for batch in result_set {
        let first_column = batch.column(0);
        match first_column.data_type() {
            arrow::datatypes::DataType::List(field) => match field.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = first_column
                        .as_any()
                        .downcast_ref::<ListArray>()
                        .ok_or_else(|| OxenError::basic_str("Failed to downcast to ListArray"))?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(OxenError::basic_str(
                                "All embeddings must be the same length",
                            ));
                        }
                    } else {
                        return Err(OxenError::basic_str(
                            "Expected Float32Array inside ListArray",
                        ));
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Expected arrow::datatypes::DataType::Float32 inside List",
                    ))
                }
            },
            arrow::datatypes::DataType::FixedSizeList(field, _) => match field.data_type() {
                arrow::datatypes::DataType::Float32 => {
                    let array = first_column
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            OxenError::basic_str("Failed to downcast to FixedSizeListArray")
                        })?;
                    if let Some(first_value) =
                        array.value(0).as_any().downcast_ref::<Float32Array>()
                    {
                        embeddings.push(first_value.values().to_vec());
                        if vector_length == 0 {
                            vector_length = first_value.len();
                        } else if first_value.len() != vector_length {
                            return Err(OxenError::basic_str(
                                "All embeddings must be the same length",
                            ));
                        }
                    }
                }
                _ => {
                    return Err(OxenError::basic_str(
                        "Column FixedSizeList must be a float32 type",
                    ))
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Expected arrow::datatypes::DataType::List inside as data type",
                ))
            }
        }
    }

    if embeddings.is_empty() {
        return Err(OxenError::basic_str("No embeddings found"));
    }

    if vector_length == 0 {
        return Err(OxenError::basic_str(
            "Vector's must have a length greater than 0",
        ));
    }

    // Average the embeddings along the columns
    let mut avg_embedding = vec![0.0; vector_length];
    for i in 0..vector_length {
        let sum: f32 = embeddings.iter().map(|v| v[i]).sum();
        avg_embedding[i] = sum / embeddings.len() as f32;
    }

    Ok(avg_embedding)
}
