
pub trait HTTPConfig<'a> {
  fn host(&'a self) -> &'a str;
  fn auth_token(&'a self) -> &'a str;
}