use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn commits_db() -> Scope {
    web::scope("/commits_db").route("", web::get().to(controllers::commits::download_commits_db))
}
