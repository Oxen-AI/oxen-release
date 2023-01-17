use crate::app_data::OxenAppData;

use liboxen::api;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{ListNamespacesResponse, NamespaceResponse, NamespaceView, StatusMessage};

use actix_web::{HttpRequest, HttpResponse};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespaces: Vec<NamespaceView> = api::local::namespaces::list(&app_data.path)
        .into_iter()
        .map(|name| NamespaceView { name })
        .collect();

    let view = ListNamespacesResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_FOUND),
        namespaces,
    };

    HttpResponse::Ok().json(view)
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");

    if let Some(namespace) = namespace {
        match api::local::namespaces::get(&app_data.path, namespace) {
            Ok(Some(namespace)) => HttpResponse::Ok().json(NamespaceResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
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
