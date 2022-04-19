use serde::{Deserialize, Serialize};

pub const STATUS_ERROR: &str = "error";
pub const STATUS_SUCCESS: &str = "success";
pub const MSG_RESOURCE_CREATED: &str = "resource_created";
pub const MSG_RESOURCE_FOUND: &str = "resource_found";
pub const MSG_RESOURCE_ALREADY_EXISTS: &str = "resource_already_exists";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HTTPStatusMsg {
    pub status: String,
    pub status_message: String,
}

impl HTTPStatusMsg {
    pub fn success(msg: &str) -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(msg),
        }
    }

    pub fn error(msg: &str) -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(STATUS_ERROR),
            status_message: String::from(msg),
        }
    }

    pub fn resource_missing() -> HTTPStatusMsg {
        HTTPStatusMsg {
            status: String::from(STATUS_ERROR),
            status_message: String::from("resource_not_found"),
        }
    }
}
