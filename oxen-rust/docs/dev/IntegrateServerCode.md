# Integrate Library Code into Server

This doc assumes you have written some library code and want to expose it as an HTTP API. We will be working off of the `list_branches` example from [here](AddLibraryCode.md).

The entry point to the server is in [src/server/src/main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/main.rs). We use the [actix](https://actix.rs/) library for all of the server setup. Each route is defined in the main file and attached to a controller.

In order to list the branches via HTTP we will want to add an `index` method to the [src/server/src/controllers/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/controllers/branches.rs) module. It is already committed to this file if you want to follow along in the actual code.

A simple controller method signature will just look like

```rust
pub async fn index(req: HttpRequest) -> HttpResponse
```

Feel free to browse the other implementations for more complex data processing out of the HTTP body or parameters etc.

Below is a stripped down version of the server in [main.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/main.rs) to make some concepts easier to see. We bind an `OxenAppData` object that contains the root path for all the repositories on disk. Then we add authentication to lock down access to the server. Finally an example get request for listing the branches on a repository.

```rust
let sync_dir = "/tmp/repositories";
let host = "0.0.0.0";
let port: u16 = 3000;
let data = app_data::OxenAppData::from(&sync_dir);

HttpServer::new(move || {
    App::new()
        // Bind app data so that controllers can access sync_dir
        .app_data(data.clone())
        // Add authentication
        .wrap(HttpAuthentication::bearer(auth::validator::validate))
        // Add an endpoint for listing branches
        .route(
            "/repositories/{repo_name}/branches",
            web::get().to(controllers::branches::index),
        )
})

.bind((host, port))?
.run()
.await
```

Back in the [controllers/branches.rs](https://github.com/Oxen-AI/Oxen/blob/main/src/server/src/controllers/branches.rs) module we can grab any global app data out of the request.

```rust
pub async fn index(req: HttpRequest) -> HttpResponse {
    let app_data = req.app_data::<OxenAppData>().unwrap();
    // ...
}
```

This contains the base directory looking for repositories in `app_data.path`. We can then look up the repository on disk with our `api::local::repositories` module.

```rust
let name: &str = req.match_info().get("repo_name").unwrap();
    // ...

    // We can grab the `repo_name` parameter that was defined in main.rs when binding the server endpoint
    let name: &str = req.match_info().get("repo_name").unwrap();
    // Then given a base path and a name, find the repository on disk
    match api::local::repositories::get_by_name(&app_data.path, name) {

    // ...
```

If we found the repository