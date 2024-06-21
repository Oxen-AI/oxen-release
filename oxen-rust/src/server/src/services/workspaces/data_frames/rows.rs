use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn rows() -> Scope {
    web::scope("/rows")
        .route(
            "/{row_id}/restore/{resource:.*}",
            web::post().to(controllers::workspaces::data_frames::rows::restore),
        )
        .route(
            "/resource/{resource:.*}",
            web::post().to(controllers::workspaces::data_frames::rows::create),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::put().to(controllers::workspaces::data_frames::rows::update),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::delete().to(controllers::workspaces::data_frames::rows::delete),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::get().to(controllers::workspaces::data_frames::rows::get),
        )
}
