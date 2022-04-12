

use std::env;

pub fn host() -> String {
    let host = env::var("HOST")
        .expect("env HOST must be set");
    host
}

pub fn port() -> String {
    let port = env::var("PORT")
        .expect("env PORT must be set");
    port
}

pub fn server() -> String {
    format!("{}:{}", host(), port())
}

pub fn url_from(name: &str) -> String {
    format!("http://{}/{}", server(), name)
}
