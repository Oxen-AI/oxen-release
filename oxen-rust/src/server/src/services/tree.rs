use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn tree() -> Scope {
    web::scope("/tree/nodes")
        .route("", web::post().to(controllers::tree::create_node))
        .service(
            web::scope("/{node_id}")
                .route("", web::get().to(controllers::tree::get_node_by_id))
                .route(
                    "/missing_file_hashes",
                    web::get().to(controllers::tree::list_missing_file_hashes),
                ),
        )
}
