use crate::app_data::OxenAppData;
use crate::controllers::entries::PageNumQuery;

use liboxen::model::{Commit, LocalRepository};
use liboxen::view::{PaginatedDirEntriesResponse, StatusMessage};
use liboxen::{api, constants};

use actix_web::{web, HttpRequest, HttpResponse};

use std::path::{Path, PathBuf};

pub async fn get(req: HttpRequest, query: web::Query<PageNumQuery>) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();

    let namespace: &str = req.match_info().get("namespace").unwrap();
    let name: &str = req.match_info().get("repo_name").unwrap();
    let resource: PathBuf = req.match_info().query("resource").parse().unwrap();

    let page: usize = query.page.unwrap_or(constants::DEFAULT_PAGE_NUM);
    let page_size: usize = query.page_size.unwrap_or(constants::DEFAULT_PAGE_SIZE);

    log::debug!(
        "dir::get repo name [{}] resource [{:?}] page {} page_size {}",
        name,
        resource,
        page,
        page_size,
    );
    match api::local::repositories::get_by_namespace_and_name(&app_data.path, namespace, name) {
        Ok(Some(repo)) => {
            if let Ok(Some((commit_id, revision, filepath))) =
                api::local::resource::parse_resource(&repo, &resource)
            {
                log::debug!(
                    "dir::get commit_id [{}] and filepath {:?}",
                    commit_id,
                    filepath
                );
                match list_directory_for_commit(
                    &repo, &commit_id, &revision, &filepath, page, page_size,
                ) {
                    Ok((entries, _commit)) => HttpResponse::Ok().json(entries),
                    Err(status_message) => HttpResponse::InternalServerError().json(status_message),
                }
            } else {
                log::debug!("dir::get could not find resource from uri {:?}", resource);
                HttpResponse::NotFound().json(StatusMessage::resource_not_found())
            }
        }
        Ok(None) => {
            log::debug!("dir::get could not find repo with name {}", name);
            HttpResponse::NotFound().json(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!("unable to get directory {:?}. Err: {}", resource, err);
            HttpResponse::InternalServerError().json(StatusMessage::internal_server_error())
        }
    }
}

fn list_directory_for_commit(
    repo: &LocalRepository,
    commit_id: &str,
    revision: &str,
    directory: &Path,
    page: usize,
    page_size: usize,
) -> Result<(PaginatedDirEntriesResponse, Commit), StatusMessage> {
    match api::local::commits::get_by_id(repo, commit_id) {
        Ok(Some(commit)) => {
            log::debug!(
                "list_directory_for_commit got commit [{}] '{}'",
                commit.id,
                commit.message
            );
            match api::local::entries::list_directory(
                repo, &commit, directory, revision, page, page_size,
            ) {
                Ok(paginated_entries) => {
                    log::debug!(
                        "list_directory_for_commit commit {} got total_entries {} entries.len() {}",
                        commit_id,
                        paginated_entries.total_entries,
                        paginated_entries.entries.len()
                    );

                    let view = PaginatedDirEntriesResponse::ok_from(paginated_entries);
                    Ok((view, commit))
                }
                Err(err) => {
                    log::error!("Unable to list repositories. Err: {}", err);
                    Err(StatusMessage::internal_server_error())
                }
            }
        }
        Ok(None) => {
            log::debug!(
                "list_directory_for_commit Could not find commit with id {}",
                commit_id
            );

            Err(StatusMessage::resource_not_found())
        }
        Err(err) => {
            log::error!(
                "list_directory_for_commit Unable to get commit id {}. Err: {}",
                commit_id,
                err
            );
            Err(StatusMessage::internal_server_error())
        }
    }
}

#[cfg(test)]
mod tests {
    use actix_web::{web, App};
    use std::path::Path;

    use liboxen::command;
    use liboxen::error::OxenError;
    use liboxen::util;
    use liboxen::view::PaginatedDirEntries;

    use crate::app_data::OxenAppData;
    use crate::controllers;
    use crate::test;
    #[actix_web::test]
    async fn test_controllers_dir_list_directory() -> Result<(), OxenError> {
        test::init_test_env();

        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Name";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;

        // write files to dir
        liboxen::test::populate_dir_with_training_data(&repo.path)?;

        // add the full dir
        let train_dir = repo.path.join(Path::new("train"));
        let num_entries = util::fs::rcount_files_in_dir(&train_dir);
        command::add(&repo, &train_dir)?;

        // commit the changes
        let commit = command::commit(&repo, "adding training dir")?;

        // Use the api list the files from the commit
        let uri = format!("/oxen/{}/{}/dir/{}/train/", namespace, name, commit.id);
        let app = actix_web::test::init_service(
            App::new()
                .app_data(OxenAppData {
                    path: sync_dir.clone(),
                })
                .route(
                    "/oxen/{namespace}/{repo_name}/dir/{resource:.*}",
                    web::get().to(controllers::dir::get),
                ),
        )
        .await;

        let req = actix_web::test::TestRequest::get().uri(&uri).to_request();
        let resp = actix_web::test::call_service(&app, req).await;
        println!("GOT RESP STATUS: {}", resp.response().status());
        let bytes = actix_http::body::to_bytes(resp.into_body()).await.unwrap();
        let body = std::str::from_utf8(&bytes).unwrap();
        println!("GOT BODY: {body}");
        let entries_resp: PaginatedDirEntries = serde_json::from_str(body)?;

        // Make sure we can fetch all the entries
        assert_eq!(entries_resp.total_entries, num_entries);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
