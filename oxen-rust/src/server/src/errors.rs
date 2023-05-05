use actix_web::{error, http::StatusCode, HttpResponse};
use derive_more::{Display, Error};
use liboxen::error::{OxenError, StringError};
use liboxen::view::{StatusMessage, StatusMessageDescription};

#[derive(Debug, Display, Error)]
pub enum OxenHttpError {
    InternalServerError,
    BadRequest,
    NotFound,
    AppDataDoesNotExist,
    PathParamDoesNotExist(StringError),

    // Translate OxenError to OxenHttpError
    InternalOxenError(OxenError),

    // External
    ActixError(actix_web::Error),
    SerdeError(serde_json::Error)
}

impl From<OxenError> for OxenHttpError {
    fn from(error: OxenError) -> Self {
        OxenHttpError::InternalOxenError(error)
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

impl error::ResponseError for OxenHttpError {
    fn error_response(&self) -> HttpResponse {
        match self {
            OxenHttpError::InternalServerError => {
                HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
            }
            OxenHttpError::BadRequest => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
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
            OxenHttpError::SerdeError(_) => {
                HttpResponse::BadRequest().json(StatusMessage::bad_request())
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
                    OxenError::CommittishNotFound(commit_id) => {
                        log::debug!("Not found: {}", commit_id);

                        HttpResponse::NotFound().json(StatusMessageDescription::not_found(format!(
                            "'{}' not found",
                            commit_id
                        )))
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
            OxenHttpError::BadRequest => StatusCode::BAD_REQUEST,
            OxenHttpError::NotFound => StatusCode::NOT_FOUND,
            OxenHttpError::ActixError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            OxenHttpError::SerdeError(_) => StatusCode::BAD_REQUEST,
            OxenHttpError::InternalOxenError(error) => match error {
                OxenError::RepoNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::CommittishNotFound(_) => StatusCode::NOT_FOUND,
                OxenError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
                OxenError::ParsingError(_) => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
        }
    }
}
