use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn embeddings() -> Scope {
    web::scope("/embeddings")
        .route(
            "/columns/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::embeddings::get),
        )
        .route(
            "/columns/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::embeddings::post),
        )
        .route(
            "/neighbors/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::embeddings::neighbors),
        )
}
