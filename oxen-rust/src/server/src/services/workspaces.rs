use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub mod data_frames;

pub fn workspace() -> Scope {
    web::scope("/workspaces")
        .route("", web::put().to(controllers::workspaces::get_or_create))
        .service(
            web::scope("/{workspace_id}")
                .route(
                    "/status/{resource:.*}",
                    web::get().to(controllers::workspaces::status_dir),
                )
                .route(
                    "/entries/{resource:.*}",
                    web::post().to(controllers::workspaces::add_file),
                )
                .route(
                    "/entries/{resource:.*}",
                    web::delete().to(controllers::workspaces::delete_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::get().to(controllers::workspaces::get_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::post().to(controllers::workspaces::add_file),
                )
                .route(
                    "/file/{resource:.*}",
                    web::delete().to(controllers::workspaces::delete_file),
                )
                .route(
                    "/diff/{resource:.*}",
                    web::get().to(controllers::workspaces::diff_file),
                )
                .route(
                    "/modifications/{resource:.*}",
                    web::delete().to(controllers::workspaces::clear_modifications),
                )
                .route(
                    "/commit/{resource:.*}",
                    web::post().to(controllers::workspaces::commit),
                )
                .service(data_frames::data_frames()),
        )
}
