use crate::errors::OxenHttpError;
use crate::helpers::get_repo;
use crate::params::{app_data, path_param};

use actix_web::{HttpRequest, HttpResponse};

use liboxen::api;
use liboxen::error::OxenError;
use liboxen::view::{
    BranchNewFromExisting, BranchResponse, BranchUpdate, ListBranchesResponse, StatusMessage,
};

pub async fn index(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let repo = get_repo(&app_data.path, namespace, name)?;

    let branches = api::local::branches::list(&repo)?;

    let view = ListBranchesResponse {
        status: StatusMessage::resource_found(),
        branches,
    };
    Ok(HttpResponse::Ok().json(view))
}

pub async fn show(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let branch = api::local::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    let view = BranchResponse {
        status: StatusMessage::resource_created(),
        branch,
    };

    Ok(HttpResponse::Ok().json(view))
}

pub async fn create_from_or_get(
    req: HttpRequest,
    body: String,
) -> Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let repo_name = path_param(&req, "repo_name")?;

    let repo = get_repo(&app_data.path, namespace, repo_name)?;

    let data: BranchNewFromExisting = serde_json::from_str(&body)?;

    let maybe_new_branch = api::local::branches::get_by_name(&repo, &data.new_name)?;
    if let Some(branch) = maybe_new_branch {
        let view = BranchResponse {
            status: StatusMessage::resource_found(),
            branch,
        };
        return Ok(HttpResponse::Ok().json(view));
    }

    let from_branch = api::local::branches::get_by_name(&repo, &data.from_name)?
        .ok_or(OxenHttpError::NotFound)?;

    let new_branch = api::local::branches::create(&repo, &data.new_name, &from_branch.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_created(),
        branch: new_branch,
    }))
}

pub async fn delete(req: HttpRequest) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let branch = api::local::branches::get_by_name(&repository, &branch_name)?
        .ok_or(OxenError::remote_branch_not_found(&branch_name))?;

    api::local::branches::force_delete(&repository, &branch.name)?;
    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_deleted(),
        branch,
    }))
}

pub async fn update(
    req: HttpRequest,
    body: String,
) -> actix_web::Result<HttpResponse, OxenHttpError> {
    let app_data = app_data(&req)?;
    let namespace = path_param(&req, "namespace")?;
    let name = path_param(&req, "repo_name")?;
    let branch_name = path_param(&req, "branch_name")?;
    let repository = get_repo(&app_data.path, namespace, name)?;

    let data: Result<BranchUpdate, serde_json::Error> = serde_json::from_str(&body);
    let data = data.map_err(|err| OxenHttpError::BadRequest(format!("{:?}", err).into()))?;

    let branch = api::local::branches::update(&repository, &branch_name, &data.commit_id)?;

    Ok(HttpResponse::Ok().json(BranchResponse {
        status: StatusMessage::resource_updated(),
        branch,
    }))
}

#[cfg(test)]
mod tests {

    use actix_web::http::{self};

    use actix_web::body::to_bytes;

    use liboxen::api;
    use liboxen::constants::DEFAULT_BRANCH_NAME;
    use liboxen::error::OxenError;
    use liboxen::view::http::STATUS_SUCCESS;
    use liboxen::view::{BranchNewFromExisting, BranchResponse, ListBranchesResponse};

    use crate::controllers;
    use crate::test;

    #[actix_web::test]
    async fn test_controllers_branches_index_empty() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        test::create_local_repo(&sync_dir, namespace, name)?;
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::branches::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        assert_eq!(list.status.status, STATUS_SUCCESS);
        // Should have main branch initialized
        assert_eq!(list.branches.len(), 1);
        assert_eq!(list.branches.first().unwrap().name, DEFAULT_BRANCH_NAME);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branches_index_multiple_branches() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, name)?;
        api::local::branches::create_from_head(&repo, "branch-1")?;
        api::local::branches::create_from_head(&repo, "branch-2")?;

        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::branches::index(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let list: ListBranchesResponse = serde_json::from_str(text)?;
        // main + branch-1 + branch-2
        assert_eq!(list.branches.len(), 3);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branch_show() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let repo_name = "Testing-Branches-1";
        let repo = test::create_local_repo(&sync_dir, namespace, repo_name)?;
        let branch_name = "branch-1";
        api::local::branches::create_from_head(&repo, branch_name)?;

        let uri = format!("/oxen/{namespace}/{repo_name}/branches");
        let req = test::repo_request_with_param(
            &sync_dir,
            &uri,
            namespace,
            repo_name,
            "branch_name",
            branch_name,
        );

        let resp = controllers::branches::show(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();
        let branch_resp: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(branch_resp.branch.name, branch_name);

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }

    #[actix_web::test]
    async fn test_controllers_branch_create() -> Result<(), OxenError> {
        let sync_dir = test::get_sync_dir()?;

        let namespace = "Testing-Namespace";
        let name = "Testing-Branches-Create";
        test::create_local_repo(&sync_dir, namespace, name)?;

        let new_name = "My-Branch-Name";

        let params = BranchNewFromExisting {
            new_name: new_name.to_string(),
            from_name: DEFAULT_BRANCH_NAME.to_string(),
        };
        let uri = format!("/oxen/{namespace}/{name}/branches");
        let req = test::repo_request(&sync_dir, &uri, namespace, name);

        let resp = controllers::branches::create_from_or_get(req, serde_json::to_string(&params)?)
            .await
            .map_err(|_err| OxenError::basic_str("OxenHttpError - could not create branch"))?;
        assert_eq!(resp.status(), http::StatusCode::OK);
        let body = to_bytes(resp.into_body()).await.unwrap();
        let text = std::str::from_utf8(&body).unwrap();

        let repo_response: BranchResponse = serde_json::from_str(text)?;
        assert_eq!(repo_response.status.status, STATUS_SUCCESS);
        assert_eq!(repo_response.branch.name, "My-Branch-Name");

        // cleanup
        std::fs::remove_dir_all(sync_dir)?;

        Ok(())
    }
}
