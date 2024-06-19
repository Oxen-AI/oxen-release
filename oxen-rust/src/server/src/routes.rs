use super::controllers;

use actix_web::web;

use crate::services;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route("", web::post().to(controllers::repositories::create))
        .route(
            "/{namespace}",
            web::get().to(controllers::repositories::index),
        )
        .service(
            web::resource("/{namespace}/{repo_name}")
                // we give the resource a name here so it can be used with HttpRequest.url_for
                .name("repo_root")
                .route(web::get().to(controllers::repositories::show))
                .route(web::delete().to(controllers::repositories::delete)),
        )
        .route(
            "/{namespace}/{repo_name}/transfer",
            web::patch().to(controllers::repositories::transfer_namespace),
        )
        .route(
            "/{namespace}/{repo_name}/commits_db",
            web::get().to(controllers::commits::download_commits_db),
        )
        .route(
            "/{namespace}/{repo_name}/objects_db",
            web::get().to(controllers::commits::download_objects_db),
        )
        .service(
            web::scope("/{namespace}/{repo_name}")
                .service(services::branches())
                .service(services::commits())
                .service(services::compare())
                .service(services::data_frame())
                .service(services::dir())
                .service(services::file())
                .service(services::merge())
                .service(services::meta())
                .service(services::revisions())
                .service(services::workspace())
                // Chunk
                .route(
                    "/chunk/{resource:.*}",
                    web::get().to(controllers::entries::download_chunk),
                )
                // Lines
                .route(
                    "/lines/{resource:.*}",
                    web::get().to(controllers::entries::list_lines_in_file),
                )
                // Versions
                .route(
                    "/versions",
                    web::get().to(controllers::entries::download_data_from_version_paths),
                )
                // Schemas
                .route(
                    "/schemas/hash/{hash}",
                    web::get().to(controllers::schemas::get_by_hash),
                )
                .route(
                    "/schemas/{resource:.*}",
                    web::get().to(controllers::schemas::list_or_get),
                )
                // Tabular
                .route(
                    "/tabular/{commit_or_branch:.*}",
                    web::get().to(controllers::entries::list_tabular),
                )
                // Stats
                .route("/stats", web::get().to(controllers::repositories::stats))
                // Action Callbacks
                .route(
                    "/action/completed/{action}",
                    web::get().to(controllers::action::completed),
                )
                .route(
                    "/action/started/{action}",
                    web::get().to(controllers::action::started),
                )
                .route(
                    "/action/completed/{action}",
                    web::post().to(controllers::action::completed),
                )
                .route(
                    "/action/started/{action}",
                    web::post().to(controllers::action::started),
                ),
        );
}
