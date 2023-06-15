use serde::{Deserialize, Serialize};

use crate::model::schema::DataType;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    pub name: String,
    pub dtype: String,
}

impl PartialEq for Field {
    fn eq(&self, other: &Field) -> bool {
        self.name == other.name && self.dtype == other.dtype
    }
}

impl Field {
    pub fn to_sql(&self) -> String {
        let dtype = DataType::from_string(&self.dtype).to_sql();
        format!("{} {}", self.name, dtype)
    }

    pub fn all_fields_to_string<V: AsRef<Vec<Field>>>(fields: V) -> String {
        let names: Vec<String> = fields.as_ref().iter().map(|f| f.name.to_owned()).collect();

        let combined_names = names.join(", ");

        format!("[{combined_names}]")
    }

    pub fn fields_to_string_with_limit<V: AsRef<Vec<Field>>>(fields: V) -> String {
        let fields = fields.as_ref();
        let max_num = 4;
        if fields.len() > max_num {
            let name_0 = fields[0].name.to_owned();
            let name_1 = fields[1].name.to_owned();
            let name_2 = fields[fields.len() - 2].name.to_owned();
            let name_3 = fields[fields.len() - 1].name.to_owned();

            let combined_names = [name_0, name_1, String::from("..."), name_2, name_3].join(", ");
            format!("[{combined_names}]")
        } else {
            let names: Vec<String> = fields.iter().map(|f| f.name.to_owned()).collect();

            let combined_names = names.join(", ");

            format!("[{combined_names}]")
        }
    }
}
