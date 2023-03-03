use crate::app_data::OxenAppData;
use actix_web::{HttpRequest, HttpResponse};
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{HealthResponse, StatusMessage};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    match util::fs::disk_usage_for_path(&app_data.path) {
        Ok(disk_usage) => {
            let response = HealthResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                disk_usage,
            };
            HttpResponse::Ok().json(response)
        }
        Err(err) => {
            log::error!("Error getting disk usage: {:?}", err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}
