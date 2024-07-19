use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn columns() -> Scope {
    web::scope("/columns")
        // .route(
        //     "/{row_id}/restore/{path:.*}",
        //     web::post().to(controllers::workspaces::data_frames::columns::restore),
        // )
        .route(
            "/resource/{path:.*}",
            web::post().to(controllers::workspaces::data_frames::columns::create),
        )
    // .route(
    //     "/{row_id}/resource/{path:.*}",
    //     web::put().to(controllers::workspaces::data_frames::rows::update),
    // )
    // .route(
    //     "/{row_id}/resource/{path:.*}",
    //     web::delete().to(controllers::workspaces::data_frames::rows::delete),
    // )
    // .route(
    //     "/{row_id}/resource/{path:.*}",
    //     web::get().to(controllers::workspaces::data_frames::rows::get),
    // )
}
