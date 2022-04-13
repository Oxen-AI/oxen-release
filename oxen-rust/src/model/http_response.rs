
use serde::{Deserialize, Serialize};

pub const STATUS_ERROR: &str = "error";
pub const STATUS_SUCCESS: &str = "success";
pub const MSG_RESOURCE_CREATED: &str = "resource_created";
pub const MSG_RESOURCE_FOUND: &str = "resource_found";
pub const MSG_RESOURCE_ALREADY_EXISTS: &str = "resource_already_exists";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HTTPErrorMsg {
    pub status: String,
    pub message: String,
}

impl HTTPErrorMsg {
    pub fn with_message(msg: &str) -> HTTPErrorMsg {
        HTTPErrorMsg {
            status: String::from(STATUS_ERROR),
            message: String::from(msg),
        }
    }

    pub fn resource_missing() -> HTTPErrorMsg {
        HTTPErrorMsg {
            status: String::from(STATUS_ERROR),
            message: String::from("resource_not_found"),
        }
    }
}
