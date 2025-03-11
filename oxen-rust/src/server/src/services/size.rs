use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn size() -> Scope {
    web::scope("/size")
        .route("", web::post().to(controllers::repositories::update_size))
        .route("", web::get().to(controllers::repositories::get_size))
}
