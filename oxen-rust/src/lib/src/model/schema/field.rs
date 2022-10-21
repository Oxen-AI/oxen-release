use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Field {
    pub name: String,
    pub dtype: String,
}

impl Field {
    pub fn fields_to_string<V: AsRef<Vec<Field>>>(fields: V) -> String {
        let fields = fields.as_ref();
        let max_num = 4;
        let names: Vec<String> = fields
            .iter()
            .take(max_num)
            .map(|f| f.name.to_owned())
            .collect();

        let combined_names = names.join(", ");
        if fields.len() > max_num {
            format!("[{}, ...]", combined_names)
        } else {
            format!("[{}]", combined_names)
        }
    }
}
