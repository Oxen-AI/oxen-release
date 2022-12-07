use actix_web::web;

use super::controllers;

pub fn config(cfg: &mut web::ServiceConfig) {
    cfg.route(
        "",
        web::post().to(controllers::repositories::create),
    )
    .route(
        "/{namespace}",
        web::get().to(controllers::repositories::index),
    )
    .service(web::resource("/{namespace}/{repo_name}")
        // we give the resource a name here so it can be used with HttpRequest.url_for
        .name("repo_root")
        .route(web::get().to(controllers::repositories::show))
        .route(web::delete().to(controllers::repositories::delete))
    )
    // ----- Commits ----- //
    .route(
        "/{namespace}/{repo_name}/commits",
        web::get().to(controllers::commits::index),
    )
    .route(
        "/{namespace}/{repo_name}/commits",
        web::post().to(controllers::commits::create),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_id}",
        web::get().to(controllers::commits::show),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_id}/data",
        web::post().to(controllers::commits::upload),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_or_branch:.*}/history",
        web::get().to(controllers::commits::commit_history),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_or_branch:.*}/parents",
        web::get().to(controllers::commits::parents),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_or_branch:.*}/is_synced",
        web::get().to(controllers::commits::is_synced),
    )
    .route(
        "/{namespace}/{repo_name}/commits/{commit_or_branch:.*}/commit_db",
        web::get().to(controllers::commits::download_commit_db),
    )
    // ----- Branches ----- //
    .route(
        "/{namespace}/{repo_name}/branches",
        web::get().to(controllers::branches::index),
    )
    .route(
        "/{namespace}/{repo_name}/branches",
        web::post().to(controllers::branches::create_or_get),
    )
    .route(
        "/{namespace}/{repo_name}/branches/{branch_name:.*}",
        web::get().to(controllers::branches::show),
    )
    .route(
        "/{namespace}/{repo_name}/branches/{branch_name:.*}",
        web::delete().to(controllers::branches::delete),
    )
    .route(
        "/{namespace}/{repo_name}/branches/{branch_name:.*}",
        web::put().to(controllers::branches::update),
    )
    // ----- Dir ----- //
    .route(
        "/{namespace}/{repo_name}/dir/{resource:.*}",
        web::get().to(controllers::dir::get),
    )
    // ----- File ----- //
    .route(
        "/{namespace}/{repo_name}/file/{resource:.*}",
        web::get().to(controllers::file::get),
    )
    // ----- Versions ----- //
    .route(
        "/{namespace}/{repo_name}/versions",
        web::post().to(controllers::entries::download_content_by_ids),
    )
    // ----- Schemas ----- //
    .route(
        "/{namespace}/{repo_name}/schemas/{resource:.*}",
        web::get().to(controllers::schemas::get),
    )

    // .route(
    //     "/{namespace}/{repo_name}/commits/{commit_id}/entries",
    //     web::get().to(controllers::entries::list_entries),
    // )

    // .route(
    //     "/{namespace}/{repo_name}/branches/{branch_name}/commits",
    //     web::get().to(controllers::commits::index_branch),
    // )
    // .route(
    //     "/{namespace}/{repo_name}/commits/{commit_id}/files",
    //     web::get().to(controllers::entries::list_files_for_commit),
    // )
    // .route(
    //     "/{namespace}/{repo_name}/commits/{commit_id}/download_page",
    //     web::get().to(controllers::entries::download_page),
    // )

    .route(
        "/{namespace}/{repo_name}/entries",
        web::post().to(controllers::entries::create),
    )
    .route(
        "/{namespace}/{repo_name}/lines/{resource:.*}",
        web::get().to(controllers::entries::list_lines_in_file),
    )
    // .route(
    //     "/{namespace}/{repo_name}/branches/{branch_name}/entries/{filename:.*}",
    //     web::get().to(controllers::repositories::get_file_for_branch),
    // )
    // .route(
    //     "/{namespace}/{repo_name}/commits/{commit_id}/entries/{filename:.*}",
    //     web::get().to(controllers::repositories::get_file_for_commit_id),
    // )
    // .route(
    //     "/{namespace}/{repo_name}/files",
    //     web::get().to(controllers::entries::list_files_for_head),
    // )
    ;
}
