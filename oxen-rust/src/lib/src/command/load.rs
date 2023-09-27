use crate::command;
use commit_reader::CommitReader;
use flate2::read::GzDecoder;
use std::path::PathBuf;
use std::{fs::File, path::Path, sync::Arc};
use tar::Archive;

use crate::opts::RestoreOpts;
use crate::{
    api,
    core::index::{commit_reader, pusher::UnsyncedCommitEntries, EntryIndexer},
    error::OxenError,
    model::LocalRepository,
};

pub fn load(src_path: &Path, dest_path: &Path, no_working_dir: bool) -> Result<(), OxenError> {
    let dest_path = if dest_path.exists() {
        if dest_path.is_file() {
            return Err(OxenError::basic_str(
                "Destination path is a file, must be a directory",
            ));
        }
        dest_path.to_path_buf()
    } else {
        std::fs::create_dir_all(dest_path)?;
        dest_path.to_path_buf()
    };

    let file = File::open(src_path)?;
    let tar = GzDecoder::new(file);
    let mut archive = Archive::new(tar);
    archive.unpack(&dest_path)?;

    // Server repos - done unpacking
    if no_working_dir {
        println!("exiting bc no working dir");
        return Ok(());
    }

    println!("continuing on, we want working dir");

    // Client repos - need to hydrate working dir from versions files
    let repo = LocalRepository::new(&dest_path)?;

    // TODONOW make sure this isn't backwards now
    // Get commit history from the head at which the repo was exported
    let commit_reader = CommitReader::new(&repo)?;

    // let head = api::local::commits::head_commit(&repo)?;
    // let history = api::local::commits::list_from(&repo, &head.id)?;

    // TODONOW this can go away

    // let mut unsynced_entries: Vec<UnsyncedCommitEntries> = Vec::new();
    // for commit in &history {
    //     for parent_id in &commit.parent_ids {
    //         let local_parent = commit_reader
    //             .get_commit_by_id(parent_id)?
    //             .ok_or_else(|| OxenError::local_parent_link_broken(&commit.id))?;

    //         let entries = api::local::entries::read_unsynced_entries(&repo, &local_parent, commit)?;
    //         // TODONOW maybe make this not a class method
    //         let these_entries = UnsyncedCommitEntries {
    //             commit: commit.clone(),
    //             entries: entries,
    //         };
    //         unsynced_entries.push(these_entries);
    //     }
    // }

    // // TODONOW fix silent bar
    // let indexer = EntryIndexer{repository: repo};
    // let silent_bar = Arc::new(indicatif::ProgressBar::hidden());
    // for commit_with_entries in unsynced_entries {
    //     indexer.unpack_version_files_to_working_dir(
    //         &commit_with_entries.commit,
    //         &commit_with_entries.entries,
    //         &silent_bar,
    //     )?;
    // }
    // Ok(())

    // Let's do this a bit differently...

    let status = command::status(&repo)?;

    // TODONOW fix this once wildcard changes are included -
    // should just be a restore("*")
    let mut restore_opts = RestoreOpts {
        path: PathBuf::from("/"),
        staged: false,
        is_remote: false,
        source_ref: None,
    };

    for path in status.removed_files {
        println!("Restoring staged file: {:?}", path);
        restore_opts.path = path;
        command::restore(&repo, restore_opts.clone())?;
    }

    Ok(())
}

// TODONOW: Over in entry indexer and puller, we can probably fix the
// commit-gathering process too..
