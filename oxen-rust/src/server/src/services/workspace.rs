use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod data_frame;

pub fn workspace() -> Scope {
    web::scope("/workspace")
        // .route("", web::post().to(controllers::workspace::create))
        // .route("", web::get().to(controllers::workspace::list))
        .service(
            web::scope("/{identifier}")
                .route(
                    "/status/{resource:.*}",
                    web::get().to(controllers::workspace::status_dir),
                )
                .route(
                    "/entries/{resource:.*}",
                    web::post().to(controllers::workspace::add_file),
                )
                .route(
                    "/entries/{resource:.*}",
                    web::delete().to(controllers::workspace::delete_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::get().to(controllers::workspace::get_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::post().to(controllers::workspace::add_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::delete().to(controllers::workspace::delete_file),
                )
                .route(
                    "/diff/{resource:.*}",
                    web::get().to(controllers::workspace::diff_file),
                )
                .route(
                    "/modifications/{resource:.*}",
                    web::delete().to(controllers::workspace::clear_modifications),
                )
                .route(
                    "/commit/{resource:.*}",
                    web::post().to(controllers::workspace::commit),
                )
                .service(data_frame::data_frame()),
        )
}
