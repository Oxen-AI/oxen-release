//! Core Oxen data types to convert between Polars and DuckDB DataFrames and Schemas
//!

use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    String,
    Date,
    Time,
    List(Box<DataType>),
    // TODO: implement these when needed...
    // Object(&'static str),
    // Datetime,
    // Duration,
    Null,
    Unknown,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl DataType {
    pub fn from_string(s: impl AsRef<str>) -> DataType {
        match s.as_ref() {
            "bool" => DataType::Boolean,
            "uint8" => DataType::UInt8,
            "u16" => DataType::UInt16,
            "u32" => DataType::UInt32,
            "u64" => DataType::UInt64,
            "i8" => DataType::Int8,
            "i16" => DataType::Int16,
            "int" => DataType::Int32,
            "i32" => DataType::Int32,
            "i64" => DataType::Int64,
            "float" => DataType::Float32,
            "f32" => DataType::Float32,
            "double" => DataType::Float64,
            "f64" => DataType::Float64,
            "str" => DataType::String,
            "date" => DataType::Date,
            "time" => DataType::Time,
            "null" => DataType::Null,
            _ => DataType::Unknown,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            DataType::Boolean => "bool",
            DataType::UInt8 => "uint8",
            DataType::UInt16 => "u16",
            DataType::UInt32 => "u32",
            DataType::UInt64 => "u64",
            DataType::Int8 => "i8",
            DataType::Int16 => "i16",
            DataType::Int32 => "i32",
            DataType::Int64 => "i64",
            DataType::Float32 => "f32",
            DataType::Float64 => "f64",
            DataType::String => "str",
            DataType::Date => "date",
            DataType::Time => "time",
            DataType::List(_val) => "list",
            DataType::Null => "null",
            DataType::Unknown => "?",
        }
    }

    pub fn to_polars(&self) -> polars::prelude::DataType {
        match self {
            DataType::Boolean => polars::prelude::DataType::Boolean,
            DataType::UInt8 => polars::prelude::DataType::UInt8,
            DataType::UInt16 => polars::prelude::DataType::UInt16,
            DataType::UInt32 => polars::prelude::DataType::UInt32,
            DataType::UInt64 => polars::prelude::DataType::UInt64,
            DataType::Int8 => polars::prelude::DataType::Int8,
            DataType::Int16 => polars::prelude::DataType::Int16,
            DataType::Int32 => polars::prelude::DataType::Int32,
            DataType::Int64 => polars::prelude::DataType::Int64,
            DataType::Float32 => polars::prelude::DataType::Float32,
            DataType::Float64 => polars::prelude::DataType::Float64,
            DataType::String => polars::prelude::DataType::String,
            DataType::Date => polars::prelude::DataType::Date,
            DataType::Time => polars::prelude::DataType::Time,
            DataType::List(val) => polars::prelude::DataType::List(Box::new(val.to_polars())),
            DataType::Null => polars::prelude::DataType::Null,
            DataType::Unknown => polars::prelude::DataType::Unknown,
        }
    }

    pub fn from_polars(dtype: &polars::prelude::DataType) -> Self {
        match dtype {
            polars::prelude::DataType::Boolean => DataType::Boolean,
            polars::prelude::DataType::UInt8 => DataType::UInt8,
            polars::prelude::DataType::UInt16 => DataType::UInt16,
            polars::prelude::DataType::UInt32 => DataType::UInt32,
            polars::prelude::DataType::UInt64 => DataType::UInt64,
            polars::prelude::DataType::Int8 => DataType::Int8,
            polars::prelude::DataType::Int16 => DataType::Int16,
            polars::prelude::DataType::Int32 => DataType::Int32,
            polars::prelude::DataType::Int64 => DataType::Int64,
            polars::prelude::DataType::Float32 => DataType::Float32,
            polars::prelude::DataType::Float64 => DataType::Float64,
            polars::prelude::DataType::String => DataType::String,
            polars::prelude::DataType::Null => DataType::Null,
            _ => DataType::Unknown,
        }
    }

    pub fn to_sql(&self) -> &'static str {
        match self {
            DataType::Boolean => "BOOL",
            DataType::UInt8 => "UTINYINT", // unsigned one-byte integer
            DataType::UInt16 => "USMALLINT", // unsigned two-byte integer
            DataType::UInt32 => "UINTEGER", // unsigned four-byte integer
            DataType::UInt64 => "UBIGINT", // unsigned eight-byte integer
            DataType::Int8 => "TINYINT",   // signed one-byte integer
            DataType::Int16 => "SMALLINT", // signed two-byte integer
            DataType::Int32 => "INTEGER",  // signed four-byte integer
            DataType::Int64 => "BIGINT",   // signed eight-byte integer
            DataType::Float32 => "FLOAT", // alias for REAL, single precision floating-point number (4 bytes)
            DataType::Float64 => "DOUBLE", // double-precision floating point number
            DataType::String => "VARCHAR", // variable-length character string
            DataType::Date => "DATE",     // calendar date (year, month day)
            DataType::Time => "TIME",     // time of day (no time zone)
            DataType::List(_) => panic!("TODO: implement list type for SQL"), // https://duckdb.org/docs/sql/data_types/list
            DataType::Null => "NULL",                                         // null value
            DataType::Unknown => panic!("TODO: unknown SQL type"),
        }
    }

    pub fn from_sql(s: impl AsRef<str>) -> Self {
        match s.as_ref() {
            "BOOL" => DataType::Boolean,
            "UTINYINT" => DataType::UInt8, // unsigned one-byte integer
            "USMALLINT" => DataType::UInt16, // unsigned two-byte integer
            "UINTEGER" => DataType::UInt32, // unsigned four-byte integer
            "UBIGINT" => DataType::UInt64, // unsigned eight-byte integer
            "TINYINT" => DataType::Int8,   // signed one-byte integer
            "SMALLINT" => DataType::Int16, // signed two-byte integer
            "INTEGER" => DataType::Int32,  // signed four-byte integer
            "BIGINT" => DataType::Int64,   // signed eight-byte integer
            "FLOAT" => DataType::Float32, // alias for REAL, single precision floating-point number (4 bytes)
            "DOUBLE" => DataType::Float64, // double-precision floating point number
            "VARCHAR" => DataType::String, // variable-length character string
            "DATE" => DataType::Date,     // calendar date (year, month day)
            "TIME" => DataType::Time,     // time of day (no time zone)
            "NULL" => DataType::Null,     // null value
            _ => panic!("TODO: unknown SQL type"),
        }
    }
}
