use std::env;

pub fn host() -> String {
    match env::var("HOST") {
        Ok(host) => host,
        Err(_) => String::from("0.0.0.0"),
    }
}

pub fn port() -> String {
    match env::var("POST") {
        Ok(port) => port,
        Err(_) => String::from("3000"),
    }
}

pub fn server() -> String {
    format!("{}:{}", host(), port())
}

pub fn url_from(name: &str) -> String {
    format!("http://{}{}", server(), name)
}
