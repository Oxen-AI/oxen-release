use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn fork() -> Scope {
    web::scope("/fork").route("", web::post().to(controllers::fork::fork))
}
