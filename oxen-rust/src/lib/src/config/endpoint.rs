use crate::api::endpoint;

pub fn http_endpoint(host: &str) -> String {
    let protocol = endpoint::get_protocol(host);
    format!("{protocol}://{host}/api/v1")
}
