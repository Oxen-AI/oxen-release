use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn transfer() -> Scope {
    web::scope("/transfer")
        .route(
            "",
            web::patch().to(controllers::repositories::transfer_namespace),
        )
}