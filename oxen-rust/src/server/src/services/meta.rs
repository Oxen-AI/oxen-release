use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn meta() -> Scope {
    web::scope("/meta")
        .route(
            "/agg/dir/{resource:.*}",
            web::get().to(controllers::metadata::agg_dir),
        )
        .route("/{resource:.*}", web::get().to(controllers::metadata::file))
        .route(
            "/{resource:.*}",
            web::post().to(controllers::metadata::update_metadata),
        )
}
