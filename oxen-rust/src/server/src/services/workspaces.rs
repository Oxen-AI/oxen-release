use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod data_frames;

pub fn workspace() -> Scope {
    web::scope("/workspaces")
        .route("", web::put().to(controllers::workspaces::get_or_create))
        .service(
            web::scope("/{workspace_id}")
                .route("", web::delete().to(controllers::workspaces::delete))
                .route(
                    "/status/{path:.*}",
                    web::get().to(controllers::workspaces::status_dir),
                )
                .route(
                    "/entries/{path:.*}",
                    web::post().to(controllers::workspaces::add_file),
                )
                .route(
                    "/entries/{path:.*}",
                    web::delete().to(controllers::workspaces::delete_file),
                )
                .route(
                    "/file/{path:.*}",
                    web::get().to(controllers::workspaces::get_file),
                )
                .route(
                    "/file/{path:.*}",
                    web::post().to(controllers::workspaces::add_file),
                )
                .route(
                    "/file/{path:.*}",
                    web::delete().to(controllers::workspaces::delete_file),
                )
                .route(
                    "/commit/{branch:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .service(data_frames::data_frames()),
        )
}
