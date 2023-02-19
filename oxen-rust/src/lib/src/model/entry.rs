pub mod commit_entry;
pub mod diff_entry;
pub mod dir_entry;
pub mod mod_entry;
pub mod remote_entry;
pub mod staged_entry;

pub trait ContentHashable {
    fn content_hash(&self) -> String;
}
