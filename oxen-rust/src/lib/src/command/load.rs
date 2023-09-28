use crate::command;
use flate2::read::GzDecoder;
use std::path::PathBuf;
use std::{fs::File, path::Path};
use tar::Archive;

use crate::opts::RestoreOpts;
use crate::{error::OxenError, model::LocalRepository};

pub fn load(src_path: &Path, dest_path: &Path, no_working_dir: bool) -> Result<(), OxenError> {
    let done_msg: String = format!(
        "✅ Loaded {:?} to an oxen repo at {:?}",
        src_path, dest_path
    );

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
    println!("🐂 Decompressing oxen repo into {:?}", dest_path);
    let mut archive = Archive::new(tar);
    archive.unpack(&dest_path)?;

    // Server repos - done unpacking
    if no_working_dir {
        println!("{done_msg}");
        return Ok(());
    }

    // Client repos - need to hydrate working dir from versions files
    let repo = LocalRepository::new(&dest_path)?;

    let status = command::status(&repo)?;

    // TODO: This logic can be simplified to restore("*") once wildcard changes are merged
    let mut restore_opts = RestoreOpts {
        path: PathBuf::from("/"),
        staged: false,
        is_remote: false,
        source_ref: None,
    };

    println!("🐂 Unpacking files to working directory {:?}", dest_path);
    for path in status.removed_files {
        println!("Restoring removed file: {:?}", path);
        restore_opts.path = path;
        command::restore(&repo, restore_opts.clone())?;
    }

    println!("{done_msg}");
    Ok(())
}
