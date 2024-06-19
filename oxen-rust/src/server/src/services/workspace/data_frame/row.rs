use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn row() -> Scope {
    web::scope("/rows")
        .route(
            "/{row_id}/restore/{resource:.*}",
            web::post().to(controllers::workspace::data_frame::row::restore),
        )
        .route(
            "/resource/{resource:.*}",
            web::post().to(controllers::workspace::data_frame::row::create),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::put().to(controllers::workspace::data_frame::row::update),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::delete().to(controllers::workspace::data_frame::row::delete),
        )
        .route(
            "/{row_id}/resource/{resource:.*}",
            web::get().to(controllers::workspace::data_frame::row::get),
        )
}
