use std::env;

pub fn host() -> String {
    env::var("HOST").expect("env HOST must be set")
}

pub fn port() -> String {
    env::var("PORT").expect("env PORT must be set")
}

pub fn server() -> String {
    format!("{}:{}", host(), port())
}

pub fn url_from(name: &str) -> String {
    format!("http://{}/{}", server(), name)
}
