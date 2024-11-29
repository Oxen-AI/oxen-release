use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn embeddings() -> Scope {
    web::scope("/embeddings/{path:.*}")
        .route(
            "",
            web::get().to(controllers::workspaces::data_frames::embeddings::get),
        )
        .route(
            "",
            web::post().to(controllers::workspaces::data_frames::embeddings::post),
        )
}
