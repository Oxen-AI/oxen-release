use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod columns;
pub mod embeddings;
pub mod rows;

pub fn data_frames() -> Scope {
    web::scope("/data_frames")
        .route(
            "/branch/{branch:.*}",
            web::get().to(controllers::workspaces::data_frames::get_by_branch),
        )
        .route(
            "/diff/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::diff),
        )
        .route(
            "/download/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::download),
        )
        .route(
            "/resource/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::get),
        )
        .route(
            "/resource/{path:.*}",
            web::put().to(controllers::workspaces::data_frames::put),
        )
        .route(
            "/resource/{path:.*}",
            web::delete().to(controllers::workspaces::data_frames::delete),
        )
        .service(rows::rows())
        .service(columns::columns())
        .service(embeddings::embeddings())
}
