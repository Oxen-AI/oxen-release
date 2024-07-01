use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn tabular() -> Scope {
    web::scope("/tabular").route(
        "/{commit_or_branch:.*}",
        web::get().to(controllers::entries::list_tabular),
    )
}
