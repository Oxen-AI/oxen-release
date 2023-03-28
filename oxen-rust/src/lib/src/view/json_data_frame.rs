use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

use crate::model::Schema;

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataSize {
    pub height: usize,
    pub width: usize,
}

impl JsonDataSize {
    pub fn is_empty(&self) -> bool {
        self.height == 0 && self.width == 0
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataFrame {
    pub schema: Schema,
    pub slice_size: JsonDataSize,
    pub full_size: JsonDataSize,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataFrameSliceResponse {
    pub status: String,
    pub status_message: String,
    pub df: JsonDataFrame,
    pub full_size: JsonDataSize,
    pub page_number: usize,
    pub page_size: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

impl JsonDataFrame {
    pub fn empty(schema: &Schema) -> JsonDataFrame {
        JsonDataFrame {
            schema: schema.to_owned(),
            slice_size: JsonDataSize { height: 0, width: 0 },
            full_size: JsonDataSize { height: 0, width: 0 },
            data: serde_json::Value::Null,
        }
    }

    pub fn from_df(df: &mut DataFrame) -> JsonDataFrame {
        JsonDataFrame {
            schema: Schema::from_polars(&df.schema()),
            slice_size: JsonDataSize {
                height: df.height(),
                width: df.width(),
            },
            full_size: JsonDataSize {
                height: df.height(),
                width: df.width(),
            },
            data: JsonDataFrame::json_data(df),
        }
    }

    pub fn to_df(&self) -> DataFrame {
        if self.data == serde_json::Value::Null {
            return DataFrame::empty();
        } else {
            let data = self.data.to_string();
            let content = Cursor::new(data.as_bytes());
            JsonReader::new(content).finish().unwrap()
        }
    }

    pub fn from_slice(df: &mut DataFrame, full_size: JsonDataSize) -> JsonDataFrame {
        JsonDataFrame {
            schema: Schema::from_polars(&df.schema()),
            slice_size: JsonDataSize {
                height: df.height(),
                width: df.width(),
            },
            full_size,
            data: JsonDataFrame::json_data(df),
        }
    }

    fn json_data(df: &mut DataFrame) -> serde_json::Value {
        log::debug!("Serializing df: [{}]", df);

        // TODO: serialize to json
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");

        let json_str = str::from_utf8(&buffer).unwrap();

        serde_json::from_str(json_str).unwrap()
    }
}
