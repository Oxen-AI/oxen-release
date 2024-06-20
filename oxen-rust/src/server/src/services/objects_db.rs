use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn objects_db() -> Scope {
    web::scope("/objects_db")
        .route(
            "",
            web::get().to(controllers::commits::download_objects_db),
        )
}