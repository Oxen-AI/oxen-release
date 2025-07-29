use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn stats() -> Scope {
    web::scope("/stats").route("", web::get().to(controllers::repositories::stats))
}
