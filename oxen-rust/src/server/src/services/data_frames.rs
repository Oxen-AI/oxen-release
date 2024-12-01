use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn data_frames() -> Scope {
    web::scope("/data_frames")
        .route(
            "/index/{resource:.*}",
            web::post().to(controllers::data_frames::index),
        )
        .route(
            "/download/{resource:.*}",
            web::get().to(controllers::data_frames::download),
        )
        .route(
            "/resource/{resource:.*}",
            web::get().to(controllers::data_frames::get),
        )
}
