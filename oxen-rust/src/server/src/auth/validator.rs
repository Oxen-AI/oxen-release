
use crate::app_data::SyncDir;
use crate::auth;

use actix_web_httpauth::extractors::bearer::BearerAuth;
use actix_web::{dev::ServiceRequest};

pub async fn validate(req: ServiceRequest, credentials: BearerAuth) -> Result<ServiceRequest, actix_web::Error> {
    let sync_dir = req.app_data::<SyncDir>().unwrap();
    if let Ok(keygen) = auth::access_keys::KeyGenerator::new(&sync_dir.path) {
        let token = credentials.token();
        if keygen.token_is_valid(token) {
            Ok(req)
        } else {
            Err(actix_web::error::ErrorUnauthorized("unauthorized"))
        }
    } else {
        Err(actix_web::error::ErrorInternalServerError("could not get keygen"))
    }
}