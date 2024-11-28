use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn embeddings() -> Scope {
    web::scope("/embeddings").route(
        "/{path:.*}",
        web::get().to(controllers::workspaces::data_frames::embeddings::get),
    )
}
