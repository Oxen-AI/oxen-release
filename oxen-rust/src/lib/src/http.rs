
pub mod status_message;
pub mod response;

pub const STATUS_ERROR: &str = "error";
pub const STATUS_SUCCESS: &str = "success";
pub const MSG_RESOURCE_CREATED: &str = "resource_created";
pub const MSG_RESOURCE_FOUND: &str = "resource_found";
pub const MSG_RESOURCE_ALREADY_EXISTS: &str = "resource_already_exists";

pub use crate::http::status_message::StatusMessage;
