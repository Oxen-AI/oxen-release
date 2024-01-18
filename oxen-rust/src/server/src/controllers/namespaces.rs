use crate::params::app_data;

use liboxen::api;
use liboxen::view::{ListNamespacesResponse, NamespaceResponse, NamespaceView, StatusMessage};

use actix_web::{HttpRequest, HttpResponse};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = app_data(&req).unwrap();

    let namespaces: Vec<NamespaceView> = api::local::namespaces::list(&app_data.path)
        .into_iter()
        .map(|namespace| NamespaceView { namespace })
        .collect();

    let view = ListNamespacesResponse {
        status: StatusMessage::resource_found(),
        namespaces,
    };

    HttpResponse::Ok().json(view)
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = app_data(&req).unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");

    if let Some(namespace) = namespace {
        match api::local::namespaces::get(&app_data.path, namespace) {
            Ok(Some(namespace)) => HttpResponse::Ok().json(NamespaceResponse {
                status: StatusMessage::resource_found(),
                namespace,
            }),

            Ok(None) => {
                log::debug!("404 Could not find namespace: {}", namespace);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::debug!("Err finding namespace: {} => {:?}", namespace, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Could not find `namespace` param";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}
