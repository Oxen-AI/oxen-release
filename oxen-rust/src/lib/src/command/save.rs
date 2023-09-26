use std::{
    fs::File,
    path::{Path, PathBuf},
};

use bytesize::ByteSize;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

use crate::{constants::OXEN_HIDDEN_DIR, error::OxenError, model::LocalRepository, util};

pub fn save(repo: &LocalRepository, dst_str: &str) -> Result<(), OxenError> {
    // Check if "path" is a directory or file.

    // TODONOW better error handling below
    // TODONOW better conditionals, didn't actually need this
    // TODONOW totally fine to pass pathbuf
    let dst_path = Path::new(dst_str);
    println!("Dst path created for checking...");
    let output_path = if !dst_path.exists() {
        println!("Path doesn't exist, but it's cool");
        dst_path.to_path_buf()
    } else {
        match (dst_path.is_file(), dst_path.is_dir()) {
            (true, false) => {
                println!("Path is a file");
                dst_path.to_path_buf()
            }
            (false, true) => {
                println!("Path is a directory");
                dst_path.join("archive.tar.gz")
            }
            _ => return Err(OxenError::basic_str(dst_str.to_string())),
        }
    };

    println!("Made it out of path matching");
    // Create a tarball of the file or directory at "path" and save it to "output_path"
    // TODONOW explore different types of compression here

    let oxen_dir = util::fs::oxen_hidden_dir(&repo.path);
    println!("Found oxen dir");
    let tar_subdir = Path::new(OXEN_HIDDEN_DIR);
    println!("made oxen subdir");

    let enc = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = tar::Builder::new(enc);

    // TODONOW: Can we append less stuff to be more efficient
    println!("About to tar");
    println!("Looking to add from oxen dir path {:?}", oxen_dir);
    tar.append_dir_all(tar_subdir, &oxen_dir)?;
    tar.finish()?;
    println!("About to write file...");

    let buffer: Vec<u8> = tar.into_inner()?.finish()?;
    let total_size: u64 = u64::try_from(buffer.len()).unwrap_or(u64::MAX);
    println!("Compressed commit dir size is {}", ByteSize::b(total_size));

    println!("Really about to write file...");
    let mut file = File::create(output_path)?;
    file.write_all(&buffer)?; // write the tarball to the file
    println!("Wrote file");
    Ok(())
}
