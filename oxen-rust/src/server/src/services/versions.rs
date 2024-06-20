use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn versions() -> Scope {
    web::scope("/versions")
        .route(
            "",
            web::get().to(controllers::entries::download_data_from_version_paths),
    )
}