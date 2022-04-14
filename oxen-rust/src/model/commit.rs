use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use urlencoding::encode;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CommitMsg {
    pub id: String,
    pub parent_id: Option<String>,
    pub message: String,
    pub author: String,
    #[serde(with = "commit_date_format")]
    pub date: DateTime<Utc>,
}

impl CommitMsg {
    pub fn to_uri_encoded(&self) -> String {
        let message = encode(&self.message);
        let author = encode(&self.author);
        let date_str = self.date_to_str();
        let date = encode(&date_str);
        format!("commit_id={}&message={}&author={}&date={}", self.id, message, author, date)
    }

    pub fn date_to_str(&self) -> String {
        self.date.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    pub fn date_from_str(date: &str) -> DateTime<Utc> {
        let no_timezone = NaiveDateTime::parse_from_str(&date, "%Y-%m-%d %H:%M:%S").unwrap();
        DateTime::<Utc>::from_utc(no_timezone, Utc)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct CommitHead {
    pub name: String,
    pub commit_id: String,
}

#[derive(Deserialize, Debug)]
pub struct CommitMsgResponse {
    pub commit: CommitMsg,
}

#[derive(Deserialize, Debug)]
pub struct PaginatedCommitMsgs {
    pub entries: Vec<CommitMsg>,
    pub page_size: usize,
    pub page_number: usize,
    pub total_pages: usize,
    pub total_entries: usize,
}

mod commit_date_format {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &str = "%Y-%m-%d %H:%M:%S";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}
