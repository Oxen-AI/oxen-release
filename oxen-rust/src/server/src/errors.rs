use actix_web::{error, http::StatusCode, HttpResponse};
use derive_more::{Display, Error};
use liboxen::error::{OxenError, StringError};
use liboxen::view::http::{MSG_UPDATE_REQUIRED, STATUS_ERROR};
use liboxen::view::{SQLParseError, StatusMessage, StatusMessageDescription};
use serde_json::json;
use std::io;
use polars::error::PolarsError;

#[derive(Debug, Display, Error)]
pub enum OxenHttpError {
    InternalServerError,
    BadRequest(StringError),
    NotFound,
    AppDataDoesNotExist,
    PathParamDoesNotExist(StringError),
    SQLParseError(StringError),

    UpdateRequired(StringError),

    // Translate OxenError to OxenHttpError
    InternalOxenError(OxenError),

    // External
    ActixError(actix_web::Error),
    SerdeError(serde_json::Error),
    RedisError(redis::RedisError),
    PolarsError(polars::error::PolarsError),
}

impl From<OxenError> for OxenHttpError {
    fn from(error: OxenError) -> Self {
        OxenHttpError::InternalOxenError(error)
    }
}

impl From<polars::error::PolarsError> for OxenHttpError {
    fn from(error: polars::error::PolarsError) -> Self {
        OxenHttpError::PolarsError(error)
    }
}

impl From<io::Error> for OxenHttpError {
    fn from(error: io::Error) -> Self {
        OxenHttpError::InternalOxenError(OxenError::IO(error))
    }
}

impl From<redis::RedisError> for OxenHttpError {
    fn from(error: redis::RedisError) -> Self {
        OxenHttpError::RedisError(error)
    }
}

impl From<actix_web::Error> for OxenHttpError {
    fn from(error: actix_web::Error) -> Self {
        OxenHttpError::ActixError(error)
    }
}

impl From<serde_json::Error> for OxenHttpError {
    fn from(error: serde_json::Error) -> Self {
        OxenHttpError::SerdeError(error)
    }
}

impl From<std::string::FromUtf8Error> for OxenHttpError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        OxenHttpError::BadRequest(StringError::new(error.to_string()))
    }
}


impl error::ResponseError for OxenHttpError {
    fn error_response(&self) -> HttpResponse {
        match self {
            OxenHttpError::InternalServerError => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::BadRequest(desc) => HttpResponse::BadRequest()
                .json(StatusMessageDescription::bad_request(desc.to_string())),
            OxenHttpError::SQLParseError(query) => {
                HttpResponse::BadRequest().json(SQLParseError::new(query.to_string()))
            }
            OxenHttpError::AppDataDoesNotExist => {
                log::error!("AppData does not exist");
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::PathParamDoesNotExist(param) => {
                log::error!(
                    "Param {} does not exist in resource path, make sure it matches in routes.rs",
                    param
                );
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::NotFound => {
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
            OxenHttpError::ActixError(_) => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::PolarsError(error) => {
                log::error!("Polars processing error: {}", error);
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::SerdeError(_) => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
            }
            OxenHttpError::RedisError(_) => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::UpdateRequired(version) => {
                let version_str = version.to_string();
                let error_json = json!({
                    "error": {
                        "type": "update_required",
                        "title": format!("Oxen CLI out of date. Pushing to OxenHub requires version >= {version_str}.")
                    },
                    "status": STATUS_ERROR,
                    "status_message": MSG_UPDATE_REQUIRED,
                });
                HttpResponse::UpgradeRequired().json(error_json)
            }
            OxenHttpError::InternalOxenError(error) => {
                // Catch specific OxenError's and return the appropriate response
                match error {
                    OxenError::RepoNotFound(repo) => {
                        log::debug!("Repo not found: {}", repo);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Repository '{}' not found",
                            repo
                        )))
                    }
                    OxenError::ParsedResourceNotFound(resource) => {
                        log::debug!("Resource not found: {}", resource);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Resource '{}' not found",
                            resource
                        )))
                    }
                    OxenError::BranchNotFound(branch) => {
                        log::debug!("Branch not found: {}", branch);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "Branch '{}' not found",
                            branch
                        )))
                    }
                    OxenError::RevisionNotFound(commit_id) => {
                        log::debug!("Not found: {}", commit_id);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "'{}' not found",
                            commit_id
                        )))
                    }
                    OxenError::PathDoesNotExist(path) => {
                        log::debug!("Path does not exist: {}", path);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "'{}' not found",
                            path
                        )))
                    }
                    OxenError::CommitEntryNotFound(msg) => {
                        log::error!("{msg}");

                        HttpResponse::NotFound()
                            .json(StatusMessageDescription::not_found(format!("{msg}")))
                    }
                    OxenError::InvalidSchema(schema) => {
                        log::error!("Invalid schema: {}", schema);

                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            format!("Schema is invalid: '{}'", schema),
                        ))
                    }
                    OxenError::ParsingError(error) => {
                        log::error!("Parsing error: {}", error);

                        HttpResponse::BadRequest().json(StatusMessageDescription::bad_request(
                            format!("Parsing error: '{}'", error),
                        ))
                    }
                    OxenError::RemoteAheadOfLocal(desc) => {
                        log::error!("Remote ahead of local: {}", desc);

                        HttpResponse::BadRequest()
                            .json(StatusMessageDescription::bad_request(format!("{}", desc)))
                    }
                    OxenError::IncompleteLocalHistory(desc) => {
                        log::error!("Cannot push repo with incomplete local history: {}", desc);

                        HttpResponse::BadRequest()
                            .json(StatusMessageDescription::bad_request(format!("{}", desc)))
                    }
                    err => {
                        log::error!("Internal server error: {:?}", err);
                        HttpResponse::InternalServerError()
                            .json(StatusMessage::internal_server_error())
                    }
                }
            }
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            OxenHttpError::InternalServerError => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::AppDataDoesNotExist => StatusCode::BAD_REQUEST,
            OxenHttpError::PathParamDoesNotExist(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::BadRequest(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::SQLParseError(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::NotFound => StatusCode::NOT_FOUND,
            OxenHttpError::UpdateRequired(_) => StatusCode::UPGRADE_REQUIRED,
            OxenHttpError::ActixError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::SerdeError(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::RedisError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::PolarsError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::InternalOxenError(error) => match error {
                OxenError::RepoNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::RevisionNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
                OxenError::ParsingError(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}
