use crate::app_data::OxenAppData;

use liboxen::api;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{ListNamespacesResponse, NamespaceView};

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
