use crate::app_data::OxenAppData;

use liboxen::api;
use liboxen::util;
use liboxen::view::http::{
    MSG_RESOURCE_CREATED, MSG_RESOURCE_DELETED, MSG_RESOURCE_FOUND, STATUS_SUCCESS,
};
use liboxen::view::{ListRepositoryResponse, RepositoryResponse, RepositoryView, StatusMessage};

use liboxen::model::{LocalRepository, RepositoryNew};

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};
use std::path::{Path, PathBuf};

pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let namespace: Option<&str> = req.match_info().get("namespace");

    if let Some(namespace) = namespace {
        let namespace_path = &app_data.path.join(namespace);

        let repos: Vec<RepositoryView> =
            api::local::repositories::list_repos_in_namespace(namespace_path)
                .iter()
                .map(|repo| RepositoryView {
                    name: repo.dirname(),
                    namespace: namespace.to_string(),
                })
                .collect();
        let view = ListRepositoryResponse {
            status: String::from(STATUS_SUCCESS),
            status_message: String::from(MSG_RESOURCE_FOUND),
            repositories: repos,
        };
        HttpResponse::Ok().json(view)
    } else {
        let msg = "Could not find `namespace` param...";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn show(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(_)) => HttpResponse::Ok().json(RepositoryResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_FOUND),
                repository: RepositoryView {
                    namespace: String::from(namespace),
                    name: String::from(name),
                },
            }),

            Ok(None) => {
                log::debug!("404 Could not find repo: {}", name);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::debug!("Err finding repo: {} => {:?}", name, err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Could not find `name` or `namespace` param...";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn create(req: HttpRequest, body: String) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // println!("controllers::repositories::create body:\n{}", body);
    let data: Result<RepositoryNew, serde_json::Error> = serde_json::from_str(&body);
    match data {
        Ok(data) => match api::local::repositories::create_empty(&app_data.path, &data) {
            Ok(_) => HttpResponse::Ok().json(RepositoryResponse {
                status: String::from(STATUS_SUCCESS),
                status_message: String::from(MSG_RESOURCE_CREATED),
                repository: RepositoryView {
                    namespace: data.namespace.clone(),
                    name: data.name.clone(),
                },
            }),

            Err(err) => {
                println!("Err api::local::repositories::create: {:?}", err);
                log::error!("Err api::local::repositories::create: {:?}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        },
        Err(err) => {
            log::error!(
                "Err api::local::repositories::create parse error: {:?}",
                err
            );
            HttpResponse::BadRequest().json(StatusMessage::error("Invalid body."))
        }
    }
}

pub async fn delete(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: Option<&str> = req.match_info().get("namespace");
    let name: Option<&str> = req.match_info().get("repo_name");
    if let (Some(name), Some(namespace)) = (name, namespace) {
        match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
            Ok(Some(repository)) => match api::local::repositories::delete(repository) {
                Ok(_) => HttpResponse::Ok().json(StatusMessage {
                    status: String::from(STATUS_SUCCESS),
                    status_message: String::from(MSG_RESOURCE_DELETED),
                }),
                Err(err) => {
                    log::error!("Error deleting repository: {}", err);
                    HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
                }
            },
            Ok(None) => {
                log::debug!("404 Could not find repo: {}", name);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            Err(err) => {
                log::error!("Delete could not find repo: {}", err);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
        }
    } else {
        let msg = "Could not find `name` param";
        HttpResponse::BadRequest().json(StatusMessage::error(msg))
    }
}

pub async fn get_file_for_branch(req: HttpRequest) -> Result<NamedFile, actix_web::Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let branch_name: &str = req.match_info().get("branch_name").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => {
            match api::local::branches::get_by_name(&repo, branch_name) {
                Ok(Some(branch)) => p_get_file_for_commit_id(&repo, &branch.commit_id, &filepath),
                Ok(None) => {
                    log::debug!("get_file_for_branch branch_name not found {}", branch_name);
                    // gives a 404
                    Ok(NamedFile::open("")?)
                }
                Err(err) => {
                    log::error!("get_file get commit err: {:?}", err);
                    // gives a 404
                    Ok(NamedFile::open("")?)
                }
            }
        }
        Ok(None) => {
            log::debug!("404 Could not find repo: {}", repo_name);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
        Err(err) => {
            log::error!("get_file_for_branch get repo err: {:?}", err);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}

pub async fn get_file_for_commit_id(req: HttpRequest) -> Result<NamedFile, actix_web::Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    let filepath: PathBuf = req.match_info().query("filename").parse().unwrap();
    let namespace: &str = req.match_info().get("namespace").unwrap();
    let repo_name: &str = req.match_info().get("repo_name").unwrap();
    let commit_id: &str = req.match_info().get("commit_id").unwrap();
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, repo_name)
    {
        Ok(Some(repo)) => p_get_file_for_commit_id(&repo, commit_id, &filepath),
        Ok(None) => {
            log::debug!("404 Could not find repo: {}", repo_name);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
        Err(err) => {
            log::error!("get_file get repo err: {:?}", err);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}

fn p_get_file_for_commit_id(
    repo: &LocalRepository,
    commit_id: &str,
    filepath: &Path,
) -> Result<NamedFile, actix_web::Error> {
    match util::fs::version_path_for_commit_id(repo, commit_id, filepath) {
        Ok(version_path) => {
            log::debug!(
                "p_get_file_for_commit_id looking for {:?} -> {:?}",
                filepath,
                version_path
            );
            Ok(NamedFile::open(version_path)?)
        }
        Err(err) => {
            log::error!("p_get_file_for_commit_id get entry err: {:?}", err);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::constants;
    use liboxen::error::OxenError;
    use liboxen::model::{Commit, RepositoryNew};

    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{ListRepositoryResponse, RepositoryResponse};
    use time::OffsetDateTime;

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_repositories_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "repositories";
        let uri = format!("/api/repos/{}", namespace);
        let req = test::namespace_request(&sync_dir, &uri, namespace);

        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 0);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_index_multiple_repos() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Test-Namespace";
        test::create_local_repo(&sync_dir, namespace, "Testing-1")?;
        test::create_local_repo(&sync_dir, namespace, "Testing-2")?;

        let uri = format!("/api/repos/{}", namespace);
        let req = test::namespace_request(&sync_dir, &uri, namespace);
        let resp = controllers::repositories::index(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListRepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(list.repositories.len(), 2);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_show() -> Result<(), OxenError> {
        log::info!("starting test");
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Test-Namespace";
        let name = "Testing-Name";
        test::create_local_repo(&sync_dir, namespace, name)?;
        log::info!("test created local repo: {}", name);

        let uri = format!("/api/repos/{}/{}", namespace, name);
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::repositories::show(req).await;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_respositories_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;
        let timestamp = OffsetDateTime::now_utc();
        let repo_new = RepositoryNew {
            name: String::from("Testing-Name"),
            namespace: String::from("Testing-Namespace"),
            root_commit: Some(Commit {
                id: String::from("1234"),
                parent_ids: vec![],
                message: String::from(constants::INITIAL_COMMIT_MSG),
                author: String::from("Ox"),
                timestamp,
            }),
        };
        let data = serde_json::to_string(&repo_new)?;
        let req = test::request(&sync_dir, "/api/repos");

        let resp = controllers::repositories::create(req, data).await;
        println!("repo create response: {:?}", resp);
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: RepositoryResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status, STATUS_SUCCESS);
        assert_eq!(repo_response.repository.name, repo_new.name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
