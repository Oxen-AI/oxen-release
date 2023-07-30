pub mod commit_entry;
pub mod diff_entry;
pub mod diff_entry_changes;
pub mod entry_data_type;
pub mod metadata_entry;
pub mod mod_entry;
pub mod remote_entry;
pub mod staged_entry;

pub trait ContentHashable {
    fn content_hash(&self) -> String;
}
