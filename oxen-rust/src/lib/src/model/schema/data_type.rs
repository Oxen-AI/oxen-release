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
    Datetime,
    Duration,
    Time,
    // TODO: implement these when needed...
    // List(Box<DataType>),
    // Object(&'static str),
    Null,
    Unknown,
}

impl DataType {
    pub fn from_str(&self, s: &str) -> DataType {
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
            "datetime" => DataType::Datetime,
            "duration" => DataType::Duration,
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
            DataType::Datetime => "datetime",
            DataType::Duration => "duration",
            DataType::Time => "time",
            DataType::Null => "null",
            DataType::Unknown => "?",
        }
    }
}
