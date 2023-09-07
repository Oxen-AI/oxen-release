use crate::{errors::OxenHttpError, params::path_param};
use actix_web::{HttpRequest, HttpResponse};

pub async fn completed(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    log::debug!("{} action completed", action);
    Ok(HttpResponse::Ok().finish())
}

pub async fn started(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let action = path_param(&req, "action")?;
    log::debug!("{} action started", action);
    Ok(HttpResponse::Ok().finish())
}
