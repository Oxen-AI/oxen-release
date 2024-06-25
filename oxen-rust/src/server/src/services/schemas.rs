use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn schemas() -> Scope {
    web::scope("/schemas")
        .route(
            "/hash/{hash}",
            web::get().to(controllers::schemas::get_by_hash),
        )
        .route(
            "/{resource:.*}",
            web::get().to(controllers::schemas::list_or_get),
        )
}
