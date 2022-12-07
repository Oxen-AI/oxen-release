use std::io::BufWriter;
use std::str;

use polars::prelude::*;
use serde::{Deserialize, Serialize};


use crate::model::Schema;

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataShape {
    pub height: usize,
    pub width: usize,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataFrame {
    pub schema: Schema,
    pub shape: JsonDataShape,
    pub data: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonDataFrameResponse {
    pub status: String,
    pub status_message: String,
    pub df: JsonDataFrame,
}

impl JsonDataFrame {
    pub fn from_df(df: &mut DataFrame) -> JsonDataFrame {
        JsonDataFrame {
            schema: Schema::from_polars(&df.schema()),
            shape: JsonDataShape {
                height: df.height(),
                width: df.width(),
            },
            data: JsonDataFrame::json_data(df),
        }
    }

    fn json_data(df: &mut DataFrame) -> serde_json::Value {
        println!("Serializing df: [{}]", df);

        // TODO: serialize to json
        let data: Vec<u8> = Vec::new();
        let mut buf = BufWriter::new(data);

        let mut writer = JsonWriter::new(&mut buf).with_json_format(JsonFormat::Json);
        writer.finish(df).expect("Could not write df json buffer");

        let buffer = buf.into_inner().expect("Could not get buffer");
        println!("Got buf len: [{}]", buffer.len());

        let json_str = str::from_utf8(&buffer).unwrap();
        println!("Got json_str: [{}]", json_str);

        serde_json::from_str(json_str).unwrap()
    }
}
