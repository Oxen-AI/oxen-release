use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod columns;
pub mod rows;

pub fn data_frames() -> Scope {
    web::scope("/data_frames")
        .route(
            "/branch/{branch:.*}",
            web::get().to(controllers::workspaces::data_frames::get_by_branch),
        )
        .route(
            "/resource/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::get_by_resource),
        )
        .route(
            "/diff/{path:.*}",
            web::get().to(controllers::workspaces::data_frames::diff),
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
}
