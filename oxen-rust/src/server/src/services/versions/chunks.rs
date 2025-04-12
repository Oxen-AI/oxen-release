use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn chunks() -> Scope {
    web::scope("/chunks")
        .route(
            "",
            web::post().to(controllers::versions::chunks::complete),
        )
}
