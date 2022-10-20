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
    // TODO: implement these when needed...
    // List(Box<DataType>),
    // Object(&'static str),
    // Datetime,
    // Duration,
    Null,
    Unknown,
}

impl DataType {
    pub fn from_string(s: &str) -> DataType {
        match s {
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
            DataType::String => polars::prelude::DataType::Utf8,
            DataType::Date => polars::prelude::DataType::Date,
            DataType::Time => polars::prelude::DataType::Time,
            DataType::Null => polars::prelude::DataType::Null,
            DataType::Unknown => polars::prelude::DataType::Unknown,
        }
    }
}
