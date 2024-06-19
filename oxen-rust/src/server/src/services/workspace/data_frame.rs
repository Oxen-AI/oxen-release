use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod row;

pub fn data_frame() -> Scope {
    web::scope("/data_frame")
        .route(
            "/branch/{branch:.*}",
            web::get().to(controllers::workspace::data_frame::get_by_branch),
        )
        .route(
            "/resource/{resource:.*}",
            web::get().to(controllers::workspace::data_frame::get_by_resource),
        )
        .route(
            "/diff/{resource:.*}",
            web::get().to(controllers::workspace::data_frame::diff),
        )
        .route(
            "/resource/{resource:.*}",
            web::put().to(controllers::workspace::data_frame::put),
        )
        .service(row::row())
}
