use actix_web::{HttpRequest, HttpResponse};
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use serde::Serialize;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Debug)]
struct VersionResponse {
    pub status: String,
    pub status_message: String,
    pub version: String,
}

pub async fn index(_req: HttpRequest) -> HttpResponse {
    let response = VersionResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_FOUND),
        version: String::from(VERSION),
    };
    HttpResponse::Ok().json(response)
}
