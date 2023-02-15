pub mod data_type;
pub mod field;

pub use data_type::DataType;
pub use field::Field;

use crate::util::hasher;
use serde::{Deserialize, Serialize};
use std::fmt;

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

    pub fn to_polars(&self) -> polars::prelude::Schema {
        let mut schema = polars::prelude::Schema::new();
        for field in self.fields.iter() {
            let data_type = DataType::from_string(&field.dtype);
            schema.with_column(field.name.to_owned(), DataType::to_polars(&data_type))
        }

        schema
    }

    pub fn from_polars(schema: &polars::prelude::Schema) -> Schema {
        let mut fields: Vec<Field> = vec![];
        for field in schema.iter_fields() {
            let f = Field {
                name: field.name().trim().to_string(),
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

    pub fn has_all_field_names(&self, schema: &polars::prelude::Schema) -> bool {
        log::debug!(
            "matches_polars checking size {} == {}",
            self.fields.len(),
            schema.len()
        );
        if self.fields.len() != schema.len() {
            return false;
        }

        let mut has_all_fields = true;
        for field in schema.iter_fields() {
            if !self.has_field_name(&field.name) {
                has_all_fields = false;
                break;
            }
        }

        has_all_fields
    }

    pub fn has_field(&self, field: &Field) -> bool {
        self.fields
            .iter()
            .any(|f| f.name == field.name && f.dtype == field.dtype)
    }

    pub fn has_field_name(&self, name: &str) -> bool {
        self.fields.iter().any(|f| f.name == name)
    }

    pub fn get_field<S: AsRef<str>>(&self, name: S) -> Option<&Field> {
        let name = name.as_ref();
        self.fields.iter().find(|f| f.name == name)
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

    /// Compare the schemas, looking for added fields
    pub fn added_fields(&self, other: &Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![];

        // if field is in current schema but not in commit, it was added
        for current_field in self.fields.iter() {
            if !other.fields.iter().any(|f| f.name == current_field.name) {
                fields.push(current_field.clone());
            }
        }

        fields
    }

    // Compare the schemas, looking for removed fields
    pub fn removed_fields(&self, other: &Schema) -> Vec<Field> {
        let mut fields: Vec<Field> = vec![];

        // if field is in commit history but not in current, it was removed
        for commit_field in other.fields.iter() {
            if !self.fields.iter().any(|f| f.name == commit_field.name) {
                fields.push(commit_field.clone());
            }
        }

        fields
    }

    pub fn schemas_to_string<S: AsRef<Vec<Schema>>>(schemas: S) -> String {
        let schemas = schemas.as_ref();
        let mut table = comfy_table::Table::new();
        table.set_header(vec!["name", "hash", "fields"]);

        for schema in schemas.iter() {
            let fields_str = Field::fields_to_string_with_limit(&schema.fields);
            if let Some(name) = &schema.name {
                table.add_row(vec![name.to_string(), schema.hash.to_string(), fields_str]);
            } else {
                table.add_row(vec!["?".to_string(), schema.hash.to_string(), fields_str]);
            }
        }
        table.to_string()
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut table = comfy_table::Table::new();
        table.set_header(vec!["id", "name", "dtype"]);

        for (i, field) in self.fields.iter().enumerate() {
            let mut cells: Vec<comfy_table::Cell> = vec![];
            cells.push(comfy_table::Cell::from(format!("{i}")));
            cells.push(comfy_table::Cell::from(field.name.to_owned()));
            cells.push(comfy_table::Cell::from(field.dtype.to_owned()));
            table.add_row(cells);
        }

        write!(f, "{table}")
    }
}

#[cfg(test)]
mod tests {
    use crate::model::schema::Field;
    use crate::model::schema::Schema;

    #[test]
    fn test_schemas_to_string_one_field() {
        let schemas = vec![Schema {
            name: Some("bounding_box".to_string()),
            hash: "1234".to_string(),
            fields: vec![Field {
                name: "file".to_string(),
                dtype: "".to_string(),
            }],
        }];
        let table = Schema::schemas_to_string(schemas);
        assert_eq!(
            table,
            r"
+--------------+------+--------+
| name         | hash | fields |
+==============================+
| bounding_box | 1234 | [file] |
+--------------+------+--------+"
                .trim()
        )
    }

    #[test]
    fn test_schemas_to_string_three_fields() {
        let schemas = vec![Schema {
            name: Some("bounding_box".to_string()),
            hash: "1234".to_string(),
            fields: vec![
                Field {
                    name: "file".to_string(),
                    dtype: "str".to_string(),
                },
                Field {
                    name: "x".to_string(),
                    dtype: "i64".to_string(),
                },
                Field {
                    name: "y".to_string(),
                    dtype: "i64".to_string(),
                },
                Field {
                    name: "w".to_string(),
                    dtype: "f64".to_string(),
                },
                Field {
                    name: "h".to_string(),
                    dtype: "f64".to_string(),
                },
            ],
        }];
        let table = Schema::schemas_to_string(schemas);
        assert_eq!(
            table,
            r"
+--------------+------+----------------------+
| name         | hash | fields               |
+============================================+
| bounding_box | 1234 | [file, x, ..., w, h] |
+--------------+------+----------------------+"
                .trim()
        )
    }

    #[test]
    fn test_schemas_to_string_no_name() {
        let schemas = vec![
            Schema {
                name: Some("bounding_box".to_string()),
                hash: "1234".to_string(),
                fields: vec![
                    Field {
                        name: "file".to_string(),
                        dtype: "str".to_string(),
                    },
                    Field {
                        name: "x".to_string(),
                        dtype: "i64".to_string(),
                    },
                    Field {
                        name: "y".to_string(),
                        dtype: "i64".to_string(),
                    },
                    Field {
                        name: "w".to_string(),
                        dtype: "f64".to_string(),
                    },
                    Field {
                        name: "h".to_string(),
                        dtype: "f64".to_string(),
                    },
                ],
            },
            Schema {
                name: None,
                hash: "5432".to_string(),
                fields: vec![
                    Field {
                        name: "file".to_string(),
                        dtype: "str".to_string(),
                    },
                    Field {
                        name: "x".to_string(),
                        dtype: "i64".to_string(),
                    },
                    Field {
                        name: "y".to_string(),
                        dtype: "i64".to_string(),
                    },
                ],
            },
        ];
        let table = Schema::schemas_to_string(schemas);
        assert_eq!(
            table,
            r"
+--------------+------+----------------------+
| name         | hash | fields               |
+============================================+
| bounding_box | 1234 | [file, x, ..., w, h] |
|--------------+------+----------------------|
| ?            | 5432 | [file, x, y]         |
+--------------+------+----------------------+"
                .trim()
        )
    }
}
