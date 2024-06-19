use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn file() -> Scope {
    web::scope("/file").route("/{resource:.*}", web::get().to(controllers::file::get))
}
