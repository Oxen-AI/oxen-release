use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::util;

use actix_files::NamedFile;
use actix_web::{web, HttpRequest};
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct ImgResize {
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Download file content
pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> actix_web::Result<NamedFile, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    let version_path =
        util::fs::version_path_for_commit_id(&repo, &resource.commit.id, &resource.file_path)?;

    // TODO: refactor out of here and check for type, but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        log::debug!("img_resize {:?}", img_resize);

        log::debug!(
            "get_file_for_commit_id resizing {:?}x{:?} for {:?} -> {:?}",
            img_resize.width,
            img_resize.height,
            resource.file_path,
            version_path
        );

        let resized_path = util::fs::resized_path_for_commit_id(
            &repo,
            &resource.commit.id,
            &resource.file_path,
            img_resize.width,
            img_resize.height,
        )?;
        log::debug!("resized_path {:?}", resized_path);
        if resized_path.exists() {
            log::debug!("serving cached {:?}", resized_path);
            return Ok(NamedFile::open(resized_path)?);
        }

        let img = image::open(&version_path).unwrap();
        let resized_img = if img_resize.width.is_some() && img_resize.height.is_some() {
            img.resize_exact(
                img_resize.width.unwrap(),
                img_resize.height.unwrap(),
                image::imageops::FilterType::Lanczos3,
            )
        } else if img_resize.width.is_some() {
            img.resize(
                img_resize.width.unwrap(),
                img_resize.width.unwrap(),
                image::imageops::FilterType::Lanczos3,
            )
        } else if img_resize.height.is_some() {
            img.resize(
                img_resize.height.unwrap(),
                img_resize.height.unwrap(),
                image::imageops::FilterType::Lanczos3,
            )
        } else {
            img
        };
        resized_img.save(&resized_path).unwrap();
        log::debug!("serving {:?}", resized_path);
        return Ok(NamedFile::open(resized_path)?);
    }

    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        resource.file_path,
        version_path
    );

    Ok(NamedFile::open(version_path)?)
}
