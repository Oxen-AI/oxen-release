use crate::params::app_data;
use actix_web::{HttpRequest, HttpResponse};
use liboxen::util;
use liboxen::view::{HealthResponse, StatusMessage};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = app_data(&req).unwrap();
    match util::fs::disk_usage_for_path(&app_data.path) {
        Ok(disk_usage) => {
            let response = HealthResponse {
                status: StatusMessage::resource_found(),
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
