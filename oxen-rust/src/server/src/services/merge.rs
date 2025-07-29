use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn merge() -> Scope {
    web::scope("/merge")
        .route("/{base_head:.*}", web::get().to(controllers::merger::show))
        .route(
            "/{base_head:.*}",
            web::post().to(controllers::merger::merge),
        )
}
