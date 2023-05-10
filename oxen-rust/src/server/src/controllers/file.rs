use crate::app_data::OxenAppData;
use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::error::OxenError;
use liboxen::model::LocalRepository;
use liboxen::util;
use liboxen::view::entry::ResourceVersion;
use liboxen::view::http::{MSG_RESOURCE_FOUND, STATUS_SUCCESS};
use liboxen::view::{EntryMetaDataResponse, FileMetaData, FileMetaDataResponse, StatusMessage};
use liboxen::{api, current_function};

use actix_files::NamedFile;
use actix_web::{HttpRequest, HttpResponse};
use std::path::{Path, PathBuf};

pub async fn get(req: HttpRequest) -> Result<NamedFile, actix_web::Error> {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!("file::get repo name [{}] resource [{:?}]", name, resource,);
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, _, filepath))) =
                api::local::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "file::get commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                get_file_for_commit_id(&repo, &commit_id, &filepath)
            } else {
                log::debug!("file::get could not find resource from uri {:?}", resource);
                Ok(NamedFile::open("")?)
            }
        }
        Ok(None) => {
            log::debug!("file::get could not find repo with name {}", name);
            Ok(NamedFile::open("")?)
        }
        Err(err) => {
            log::error!("unable to get file {:?}. Err: {}", resource, err);
            Ok(NamedFile::open("")?)
        }
    }
}

pub async fn meta_data_legacy(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    log::debug!(
        "file::meta_data repo name [{}] resource [{:?}]",
        name,
        resource,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, _, filepath))) =
                api::local::resource::parse_resource(&repo, &resource)
            {
                match util::fs::version_path_for_commit_id(&repo, &commit_id, &filepath) {
                    Ok(version_path) => {
                        log::debug!(
                            "file::meta_data looking for {:?} -> {:?}",
                            filepath,
                            version_path
                        );

                        let meta = util::fs::metadata(&version_path).unwrap();

                        let meta = FileMetaDataResponse {
                            status: String::from(STATUS_SUCCESS),
                            status_message: String::from(MSG_RESOURCE_FOUND),
                            meta: FileMetaData {
                                size: meta.len(),
                                data_type: util::fs::file_datatype(&version_path),
                                resource: ResourceVersion {
                                    path: String::from(filepath.to_str().unwrap()),
                                    version: commit_id,
                                },
                            },
                        };
                        HttpResponse::Ok().json(meta)
                    }
                    Err(err) => {
                        log::error!("file::meta_data get entry err: {:?}", err);
                        // gives a 404
                        HttpResponse::NotFound().json(StatusMessage::resource_not_found())
                    }
                }
            } else {
                log::debug!(
                    "file::meta_data could not find resource from uri {:?}",
                    resource
                );
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("file::meta_data could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "file::meta_data unable to get file {:?}. Err: {}",
                resource,
                err
            );
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

pub async fn meta_data(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {}/{}",
        current_function!(),
        repo_name,
        resource
    );

    let latest_commit = api::local::commits::get_by_id(&repo, &resource.commit.id)?.ok_or(
        OxenError::committish_not_found(resource.commit.id.clone().into()),
    )?;

    log::debug!(
        "{} resolve commit {} -> '{}'",
        current_function!(),
        latest_commit.id,
        latest_commit.message
    );

    let entry = api::local::entries::get_dir_entry(&repo, &resource.commit, &resource.file_path)?;
    let meta = EntryMetaDataResponse {
        status: String::from(STATUS_SUCCESS),
        status_message: String::from(MSG_RESOURCE_FOUND),
        entry,
    };
    Ok(HttpResponse::Ok().json(meta))
}

fn get_file_for_commit_id(
    repo: &LocalRepository,
    commit_id: &str,
    filepath: &Path,
) -> Result<NamedFile, actix_web::Error> {
    match util::fs::version_path_for_commit_id(repo, commit_id, filepath) {
        Ok(version_path) => {
            log::debug!(
                "get_file_for_commit_id looking for {:?} -> {:?}",
                filepath,
                version_path
            );
            Ok(NamedFile::open(version_path)?)
        }
        Err(err) => {
            log::error!("get_file_for_commit_id get entry err: {:?}", err);
            // gives a 404
            Ok(NamedFile::open("")?)
        }
    }
}
