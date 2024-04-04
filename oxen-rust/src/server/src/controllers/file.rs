use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, parse_resource, path_param};

use liboxen::core::index::ObjectDBReader;
use liboxen::error::OxenError;
use liboxen::model::metadata::metadata_image::ImgResize;
use liboxen::model::CommitEntry;
use liboxen::util;

use actix_files::NamedFile;
use actix_web::{web, HttpRequest};

/// Download file content
pub async fn get(
    req: HttpRequest,
    query: web::Query<ImgResize>,
) -> actix_web::Result<NamedFile, OxenHttpError> {
    log::debug!("get file path {:?}", req.path());

    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, &namespace, &repo_name)?;
    let resource = parse_resource(&req, &repo)?;

    log::debug!(
        "{} resource {namespace}/{repo_name}/{resource}",
        liboxen::current_function!()
    );

    // TODO: CLEANUP and refactor so we can use the CderLRUCache in other places that might need it.

    // This logic to use a LRUCache of CommitDirEntryReaders is to avoid opening the database many times for the same commit
    // When fetching many images, it was taking over 2 seconds to just open the CelebA database
    // This way, we open it once, and reuse it for the subsequent requests

    // Try to get the parent of the file path, if it exists
    let mut entry: Option<CommitEntry> = None;
    let object_reader = ObjectDBReader::new(&repo)?;
    let path = &resource.file_path;
    if let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) {
        let key = format!(
            "{}_{}_{}_{}",
            namespace,
            repo_name,
            resource.commit.id,
            parent.display()
        );
        log::debug!("LRU key {}", key);

        let mut cache = app_data.cder_lru.write().unwrap();

        if let Some(cder) = cache.get(&key) {
            log::debug!("found in LRU");
            entry = cder.get_entry(file_name)?;
            log::debug!("got entry {} -> {:?}", key, entry);
        } else {
            log::debug!("not found in LRU");
            let cder = liboxen::core::index::CommitDirEntryReader::new(
                &repo,
                &resource.commit.id,
                parent,
                object_reader.clone(),
            )?;
            log::debug!("looking up entry {}", key);
            entry = cder.get_entry(file_name)?;
            log::debug!("got entry {} -> {:?}", key, entry);
            cache.put(key, cder);
        }
    }

    let entry = entry.ok_or(OxenError::path_does_not_exist(&resource.file_path))?;

    let version_path = util::fs::version_path(&repo, &entry);

    log::debug!("version path {version_path:?}",);

    // TODO: refactor out of here and check for type, but seeing if it works to resize the image and cache it to disk if we have a resize query
    let img_resize = query.into_inner();
    if img_resize.width.is_some() || img_resize.height.is_some() {
        log::debug!("img_resize {:?}", img_resize);

        let resized_path = util::fs::resized_path_for_commit_entry(
            &repo,
            &entry,
            img_resize.width,
            img_resize.height,
        )?;

        util::fs::resize_cache_image(&version_path, &resized_path, img_resize)?;

        log::debug!("In the resize cache! {:?}", resized_path);
        return Ok(NamedFile::open(resized_path)?);

        // log::debug!(
        //     "get_file_for_commit_id {:?}x{:?} for {:?} -> {:?}",
        //     img_resize.width,
        //     img_resize.height,
        //     resource.file_path,
        //     version_path
        // );

        // let resized_path = util::fs::resized_path_for_commit_entry(
        //     &repo,
        //     &entry,
        //     img_resize.width,
        //     img_resize.height,
        // )?;
        // log::debug!("get_file_for_commit_id resized_path {:?}", resized_path);
        // if resized_path.exists() {
        //     log::debug!("serving cached {:?}", resized_path);
        //     return Ok(NamedFile::open(resized_path)?);
        // }

        // log::debug!("get_file_for_commit_id resizing: {:?}", resized_path);

        // let img = image::open(&version_path).unwrap();
        // let resized_img = if img_resize.width.is_some() && img_resize.height.is_some() {
        //     img.resize_exact(
        //         img_resize.width.unwrap(),
        //         img_resize.height.unwrap(),
        //         image::imageops::FilterType::Lanczos3,
        //     )
        // } else if img_resize.width.is_some() {
        //     img.resize(
        //         img_resize.width.unwrap(),
        //         img_resize.width.unwrap(),
        //         image::imageops::FilterType::Lanczos3,
        //     )
        // } else if img_resize.height.is_some() {
        //     img.resize(
        //         img_resize.height.unwrap(),
        //         img_resize.height.unwrap(),
        //         image::imageops::FilterType::Lanczos3,
        //     )
        // } else {
        //     img
        // };
        // resized_img.save(&resized_path).unwrap();
        // log::debug!("get_file_for_commit_id serving {:?}", resized_path);
        // return Ok(NamedFile::open(resized_path)?);
    } else {
        log::debug!("did not hit the resize cache");
    }

    log::debug!(
        "get_file_for_commit_id looking for {:?} -> {:?}",
        resource.file_path,
        version_path
    );

    Ok(NamedFile::open(version_path)?)
}
