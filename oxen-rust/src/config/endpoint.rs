
pub fn http_endpoint(host: &str) -> String {
  format!("http://{}/api/v1", host)
}
