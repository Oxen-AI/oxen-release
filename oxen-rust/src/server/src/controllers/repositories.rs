use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::util;
use liboxen::view::http::{MSG_RESOURCE_FOUND, MSG_RESOURCE_UPDATED, STATUS_SUCCESS};
use liboxen::view::repository::{
    DataTypeView, RepositoryDataTypesResponse, RepositoryDataTypesView, RepositoryStatsResponse,
    RepositoryStatsView,
};
use liboxen::view::{
    ListRepositoryResponse, NamespaceView, RepositoryResponse, RepositoryView, StatusMessage,
};

use liboxen::model::RepoNew;

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};
use std::path::PathBuf;

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;

    let namespace_path = &app_data.path.join(&namespace);

    let repos: Vec<RepositoryView> =
        api::local::repositories::list_repos_in_namespace(namespace_path)
            .iter()
            .map(|repo| RepositoryView {
                name: repo.dirname(),
                namespace: namespace.to_string(),
            })
            .collect();
    let view = ListRepositoryResponse {
        status: StatusMessage::resource_found(),
        repositories: repos,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    // Get the repository or return error
    let _repository = get_repo(&app_data.path, &namespace, &name)?;

    // Return the repository view
    Ok(HttpResponse::Ok().json(RepositoryDataTypesResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_FOUND.to_string(),
        repository: RepositoryDataTypesView {
            namespace,
            name,
            // Right now these get enriched in the hub
            // Hacking around it to not show in CLI unless you go through hub for now
            size: 0,
            data_types: vec![],
        },
    }))
}

// Need this endpoint to get the size and data types for a repo from the UI
pub async fn stats(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repo)) => {
                let stats = api::local::repositories::get_repo_stats(&repo);
                let data_types: Vec<DataTypeView> = stats
                    .data_types
                    .values()
                    .map(|s| DataTypeView {
                        data_type: s.data_type.to_owned(),
                        file_count: s.file_count,
                        data_size: s.data_size,
                    })
                    .collect();
                Ok(HttpResponse::Ok().json(RepositoryStatsResponse {
                    status: StatusMessage::resource_found(),
                    repository: RepositoryStatsView {
                        data_size: stats.data_size,
                        data_types,
                    },
                }))
            }
            Ok(None) => {
                log::debug!("404 Could not find repo: {}", name);
                Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()))
            }
            Err(err) => {
                log::debug!("Err finding repo: {} => {:?}", name, err);
                Ok(
                    HttpResponse::InternalServerError()
                        .json(StatusMessage::internal_server_error()),
                )
            }
        }
    } else {
        let msg = "Could not find `name` or `namespace` param...";
        Ok(HttpResponse::BadRequest().json(StatusMessage::error(msg)))
    }
}

pub async fn create(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    println!("controllers::repositories::create body:\n{}", body);
    let data: Result<RepoNew, serde_json::Error> = serde_json::from_str(&body);
    match data {
        Ok(data) => match api::local::repositories::create(&app_data.path, data.to_owned()) {
            Ok(_) => Ok(HttpResponse::Ok().json(RepositoryResponse {
                status: STATUS_SUCCESS.to_string(),
                status_message: MSG_RESOURCE_FOUND.to_string(),
                repository: RepositoryView {
                    namespace: data.namespace.clone(),
                    name: data.name,
                },
            })),
            Err(OxenError::RepoAlreadyExists(path)) => {
                log::debug!("Repo already exists: {:?}", path);
                Ok(HttpResponse::Conflict().json(StatusMessage::error("Repo already exists.")))
            }
            Err(err) => {
                println!("Err api::local::repositories::create: {err:?}");
                log::error!("Err api::local::repositories::create: {:?}", err);
                Ok(HttpResponse::InternalServerError().json(StatusMessage::error("Invalid body.")))
            }
        },
        Err(err) => {
            log::error!(
                "Err api::local::repositories::create parse error: {:?}",
                err
            );
            Ok(HttpResponse::BadRequest().json(StatusMessage::error("Invalid body.")))
        }
    }
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;

    let Ok(repository) = get_repo(&app_data.path, &namespace, &name) else {
        return Ok(HttpResponse::NotFound().json(StatusMessage::resource_not_found()));
    };

    // Delete in a background thread because it could take awhile
    std::thread::spawn(move || match api::local::repositories::delete(repository) {
        Ok(_) => log::info!("Deleted repo: {}/{}", namespace, name),
        Err(err) => log::error!("Err deleting repo: {}", err),
    });

    Ok(HttpResponse::Ok().json(StatusMessage::resource_deleted()))
}

pub async fn transfer_namespace(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    // Parse body
    let from_namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let data: NamespaceView = serde_json::from_str(&body)?;
    let to_namespace = data.namespace;
    api::local::repositories::transfer_namespace(
        &app_data.path,
        &name,
        &from_namespace,
        &to_namespace,
    )?;

    // Return repository view under new namespace
    Ok(HttpResponse::Ok().json(RepositoryResponse {
        status: STATUS_SUCCESS.to_string(),
        status_message: MSG_RESOURCE_UPDATED.to_string(),
        repository: RepositoryView {
            namespace: to_namespace,
            name,
        },
    }))
}

pub async fn get_file_for_branch(req: HttpRequest) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let branch_name: &str = req.match_info().get("branch_name").unwrap();

    let branch = api::local::branches::get_by_name(&repo, branch_name)?
        .ok_or(OxenError::remote_branch_not_found(branch_name))?;
    let version_path = util::fs::version_path_for_commit_id(&repo, &branch.commit_id, &filepath)?;
    log::debug!(
        "get_file_for_branch looking for {:?} -> {:?}",
        filepath,
        version_path
    );
    Ok(NamedFile::open(version_path)?)
}

pub async fn get_file_for_commit_id(req: HttpRequest) -> Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    let version_path =
        util::fs::version_path_for_commit_id(&repo, &resource.commit.id, &resource.file_path)?;
    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        resource.file_path,
        version_path
    );
    Ok(NamedFile::open(version_path)?)
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::constants;
    use liboxen::error::OxenError;
    use liboxen::model::{Commit, RepoNew};
    use liboxen::util;

    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{ListRepositoryResponse, NamespaceView, RepositoryResponse};
    use time::OffsetDateTime;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_repositories_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();

        let namespace = "repositories";
        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, queue, &uri, namespace);

        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();

        let namespace = "Test-Namespace";
        test::create_local_repo(&sync_dir, namespace, "Testing-1")?;
        test::create_local_repo(&sync_dir, namespace, "Testing-2")?;

        let uri = format!("/api/repos/{namespace}");
        let req = test::namespace_request(&sync_dir, queue, &uri, namespace);
        let resp = controllers::repositories::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_show() -> Result<(), OxenError> {
        log::info!("starting test");
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;
        log::info!("test created local repo: {}", name);

        let uri = format!("/api/repos/{namespace}/{name}");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let resp = controllers::repositories::show(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let queue = test::init_queue();
        let timestamp = OffsetDateTime::now_utc();
        let root_commit = Commit {
            id: String::from("1234"),
            parent_ids: vec![],
            message: String::from(constants::INITIAL_COMMIT_MSG),
            author: String::from("Ox"),
            email: String::from("ox@oxen.ai"),
            timestamp,
            root_hash: None,
        };
        let repo_new = RepoNew::from_root_commit("Testing-Name", "Testing-Namespace", root_commit);
        let data = serde_json::to_string(&repo_new)?;
        let req = test::request(&sync_dir, queue, "/api/repos");

        let resp = controllers::repositories::create(req, data).await.unwrap();
        println!("repo create response: {resp:?}");
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, repo_new.name);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_repositories_transfer_namespace() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        let queue = test::init_queue();
        test::create_local_repo(&sync_dir, namespace, name)?;

        // Create new repo in a namespace so it exists
        let new_namespace = "New-Namespace";
        let new_name = "Newbie";
        test::create_local_repo(&sync_dir, new_namespace, new_name)?;

        let uri = format!("/api/repos/{namespace}/{name}/transfer");
        let req = test::repo_request(&sync_dir, queue, &uri, namespace, name);

        let params = NamespaceView {
            namespace: new_namespace.to_string(),
        };
        let resp =
            controllers::repositories::transfer_namespace(req, serde_json::to_string(&params)?)
                .await
                .unwrap();

        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;

        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);
        assert_eq!(repo_response.repository.namespace, new_namespace);

        // cleanup
        util::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
