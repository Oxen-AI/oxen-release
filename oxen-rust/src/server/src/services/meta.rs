use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn meta() -> Scope {
    web::scope("/meta").route("/{resource:.*}", web::get().to(controllers::metadata::file))
}
