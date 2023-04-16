use crate::view;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusMessage {
    pub status: String,
    pub status_message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StatusMessageDescription {
    pub status: String,
    pub status_message: String,
    pub status_description: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IsValidStatusMessage {
    pub status: String,
    pub status_message: String,
    pub status_description: String,
    pub is_processing: bool,
    pub is_valid: bool,
}

impl StatusMessageDescription {
    pub fn not_found(description: impl AsRef<str>) -> StatusMessageDescription {
        StatusMessageDescription {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_RESOURCE_NOT_FOUND),
            status_description: String::from(description.as_ref()),
        }
    }
}

impl StatusMessage {
    pub fn success(msg: &str) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(msg),
        }
    }

    pub fn error(msg: impl AsRef<str>) -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(msg.as_ref()),
        }
    }

    pub fn bad_request() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_BAD_REQUEST),
        }
    }

    pub fn resource_not_found() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_RESOURCE_NOT_FOUND),
        }
    }

    pub fn resource_created() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_CREATED),
        }
    }

    pub fn resource_deleted() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_SUCCESS),
            status_message: String::from(view::http::MSG_RESOURCE_DELETED),
        }
    }

    pub fn internal_server_error() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_INTERNAL_SERVER_ERROR),
        }
    }

    pub fn not_implemented() -> StatusMessage {
        StatusMessage {
            status: String::from(view::http::STATUS_ERROR),
            status_message: String::from(view::http::MSG_NOT_IMPLEMENTED),
        }
    }
}
