use crate::view;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusMessage {
    pub status: String,
    pub status_message: String,
}

impl StatusMessage {
    pub fn success(msg: &str) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(msg),
        }
    }

    pub fn error(msg: &str) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(msg),
        }
    }

    pub fn resource_not_found() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_RESOURCE_NOT_FOUND),
        }
    }

    pub fn internal_server_error() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_INTERNAL_SERVER_ERROR),
        }
    }
}
