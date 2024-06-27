use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn revisions() -> Scope {
    web::scope("/revisions").route("/{resource:.*}", web::get().to(controllers::revisions::get))
}
