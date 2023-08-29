use crate::errors::OxenHttpError;
use actix_web::{HttpRequest, HttpResponse};

pub async fn completed(_req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Clone action completed");
    Ok(HttpResponse::Ok().finish())
}

pub async fn started(_req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    log::debug!("Clone action started");
    Ok(HttpResponse::Ok().finish())
}
