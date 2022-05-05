use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HTTPStatusMsg {
    pub status: String,
    pub status_message: String,
}

impl HTTPStatusMsg {
    pub fn success(msg: &str) -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(liboxen::view::http::STATUS_SUCCESS),
            status_message: String::from(msg),
        }
    }

    pub fn error(msg: &str) -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(liboxen::view::http::STATUS_ERROR),
            status_message: String::from(msg),
        }
    }

    pub fn resource_missing() -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(liboxen::view::http::STATUS_ERROR),
            status_message: String::from(liboxen::view::http::MSG_RESOURCE_NOT_FOUND),
        }
    }
}
