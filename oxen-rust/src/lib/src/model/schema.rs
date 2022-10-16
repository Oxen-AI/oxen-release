pub mod data_type;
pub mod field;

pub use data_type::DataType;
pub use field::Field;

use serde::{Deserialize, Serialize};
use crate::util::hasher;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Schema {
    pub name: Option<String>,
    pub hash: String,
    pub fields: Vec<Field>,
}

impl Schema {
    pub fn from_fields(fields: Vec<Field>) -> Schema {
        Schema {
            name: None,
            hash: Schema::hash_fields(&fields),
            fields: fields.to_owned(),
        }
    }

    pub fn from_polars(schema: polars::prelude::Schema) -> Schema {
        let mut fields: Vec<Field> = vec![];
        for field in schema.iter_fields() {
            let f = Field {
                name: field.name().to_string(),
                dtype: field.data_type().to_string(),
            };
            fields.push(f);
        }

        Schema {
            name: None,
            hash: Schema::hash_fields(&fields),
            fields,
        }
    }

    fn hash_fields(fields: &Vec<Field>) -> String {
        let mut hash_buffers: Vec<String> = vec![];
        for f in fields {
            hash_buffers.push(format!("{}{}", f.name, f.dtype));
        }

        let buffer_str = hash_buffers.join("");
        let buffer = buffer_str.as_bytes();
        hasher::hash_buffer(buffer)
    }
}
