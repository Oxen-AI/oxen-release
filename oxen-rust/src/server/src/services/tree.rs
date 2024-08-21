use actix_web::web;
use actix_web::Scope;

use crate::controllers;

pub fn tree() -> Scope {
    web::scope("/tree").service(
        web::scope("/{node_id}").route("", web::get().to(controllers::tree::get_node_by_id)),
    )
}
