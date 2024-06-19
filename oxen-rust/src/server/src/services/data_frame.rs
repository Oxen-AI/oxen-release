use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn data_frame() -> Scope {
    web::scope("/data_frame")
        .route(
            "/index/{resource:.*}",
            web::post().to(controllers::data_frames::index),
        )
        .route(
            "/{resource:.*}",
            web::get().to(controllers::data_frames::get),
        )
}
