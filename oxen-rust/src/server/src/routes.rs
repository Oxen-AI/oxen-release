use actix_web::web;

use super::controllers;

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
            "/{namespace}/{repo_name}/commits/bulk",
            web::post().to(controllers::commits::create_bulk),
        )
        .route(
            "/{namespace}/{repo_name}/commits/complete",
            web::post().to(controllers::commits::complete_bulk),
        )
        .route(
            "/{namespace}/{repo_name}/commits/{commit_id}/db_status",
            web::get().to(controllers::commits::commits_db_status),
        )
        .route(
            "/{namespace}/{repo_name}/commits/{commit_id}/entries_status",
            web::get().to(controllers::commits::entries_status),
        )
        .route(
            "/{namespace}/{repo_name}/commits_db", // download the database of all the commits and their parents
            web::get().to(controllers::commits::download_commits_db),
        )
        .route(
            "/{namespace}/{repo_name}/commits/{commit_id}/latest_synced",
            web::get().to(controllers::commits::latest_synced),
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
            "/{namespace}/{repo_name}/commits/{commit_id}/complete",
            web::post().to(controllers::commits::complete),
        )
        .route(
            "/{namespace}/{repo_name}/commits/{commit_id}/upload_chunk",
            web::post().to(controllers::commits::upload_chunk),
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
            web::get().to(controllers::commits::download_commit_entries_db),
        )
        // ----- Branches ----- //
        .route(
            "/{namespace}/{repo_name}/branches",
            web::get().to(controllers::branches::index),
        )
        .route(
            "/{namespace}/{repo_name}/branches",
            web::post().to(controllers::branches::create_from_or_get),
        )
        .route(
            "/{namespace}/{repo_name}/branches/{branch_name:.*}/lock",
            web::post().to(controllers::branches::lock),
        )
        .route(
            "/{namespace}/{repo_name}/branches/{branch_name:.*}/latest_synced_commit",
            web::get().to(controllers::branches::latest_synced_commit),
        )
        .route(
            "/{namespace}/{repo_name}/branches/{branch_name:.*}/lock",
            web::get().to(controllers::branches::is_locked),
        )
        .route(
            "/{namespace}/{repo_name}/branches/{branch_name:.*}/unlock",
            web::post().to(controllers::branches::unlock),
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
        // ----- Compare ----- //
        .route(
            "/{namespace}/{repo_name}/compare/commits/{base_head:.*}",
            web::get().to(controllers::compare::commits),
        )
        .route(
            "/{namespace}/{repo_name}/compare/entries/{base_head:.*}",
            web::get().to(controllers::compare::entries),
        )
        .route(
            "/{namespace}/{repo_name}/compare/file/{base_head:.*}",
            web::get().to(controllers::compare::file),
        )
        // ----- Merge ----- //
        // GET merge to test if merge is possible
        .route(
            "/{namespace}/{repo_name}/merge/{base_head:.*}",
            web::get().to(controllers::merger::show),
        )
        // POST merge to actually merge the branches
        .route(
            "/{namespace}/{repo_name}/merge/{base_head:.*}",
            web::post().to(controllers::merger::merge),
        )
        // ----- Stage Remote Data ----- //
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/status/{resource:.*}",
            web::get().to(controllers::stager::status_dir),
        )
        // STAGING
        // TODO: add GET for downloading the file from the staging area
        // TODO: implement delete dir from staging to recursively unstage
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/entries/{resource:.*}",
            web::post().to(controllers::stager::add_file),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/entries/{resource:.*}",
            web::delete().to(controllers::stager::delete_file),
        )
        // END STAGING
        // DEPRECIATED STAGING
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/file/{resource:.*}",
            web::post().to(controllers::stager::add_file),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/file/{resource:.*}",
            web::delete().to(controllers::stager::delete_file),
        )
        // END DEPRECIATED STAGING
        // "/{namespace}/{repo_name}/staging/dir/{resource:.*}",
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/diff/{resource:.*}",
            web::get().to(controllers::stager::diff_file),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/df/rows/{resource:.*}",
            web::post().to(controllers::stager::df_add_row),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/df/rows/{resource:.*}",
            web::delete().to(controllers::stager::df_delete_row),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/modifications/{resource:.*}",
            web::delete().to(controllers::stager::clear_modifications),
        )
        .route(
            "/{namespace}/{repo_name}/staging/{identifier}/commit/{branch:.*}",
            web::post().to(controllers::stager::commit),
        )
        // ----- Dir ----- //
        .route(
            "/{namespace}/{repo_name}/dir/{resource:.*}",
            web::get().to(controllers::dir::get),
        )
        // ----- File (returns raw file data) ----- //
        .route(
            "/{namespace}/{repo_name}/file/{resource:.*}",
            web::get().to(controllers::file::get),
        )
        // ----- Chunk (returns a chunk of a file) ----- //
        .route(
            "/{namespace}/{repo_name}/chunk/{resource:.*}",
            web::get().to(controllers::entries::download_chunk),
        )
        // ----- Metadata (returns metadata for a file or a dir) ----- //
        .route(
            "/{namespace}/{repo_name}/meta/agg/dir/{resource:.*}",
            web::get().to(controllers::metadata::agg_dir),
        )
        .route(
            "/{namespace}/{repo_name}/meta/dir/{resource:.*}",
            web::get().to(controllers::metadata::dir),
        )
        .route(
            "/{namespace}/{repo_name}/meta/images/{resource:.*}",
            web::get().to(controllers::metadata::images),
        )
        .route(
            "/{namespace}/{repo_name}/meta/{resource:.*}",
            web::get().to(controllers::metadata::file),
        )
        // ----- DataFrame ----- //
        .route(
            "/{namespace}/{repo_name}/df/{resource:.*}",
            web::get().to(controllers::df::get),
        )
        // ----- Lines ----- //
        .route(
            "/{namespace}/{repo_name}/lines/{resource:.*}",
            web::get().to(controllers::entries::list_lines_in_file),
        )
        // ----- Versions - Download directly from the .oxen/versions directory ----- //
        .route(
            "/{namespace}/{repo_name}/versions", // Download tar.gz set of version files
            web::get().to(controllers::entries::download_data_from_version_paths),
        )
        // ----- Schemas ----- //
        .route(
            "/{namespace}/{repo_name}/schemas/{resource:.*}",
            web::get().to(controllers::schemas::list_or_get),
        );
}
