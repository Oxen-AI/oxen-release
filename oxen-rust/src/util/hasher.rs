
use std::fs::File;
use std::path::Path;
use std::io::{BufReader};
use std::io::prelude::*;
use sha2::{Sha256, Digest};

pub fn hash_buffer(buffer: &[u8]) -> String {
  let mut hasher = Sha256::new();
  hasher.update(&buffer);
  format!("{:X}", hasher.finalize())
}

pub fn hash_file_contents(path: &Path) -> Result<String, String> {
  match File::open(path) {
      Ok(file) => {
          let mut reader = BufReader::new(file);
          let mut buffer = Vec::new();
          match reader.read_to_end(&mut buffer) {
              Ok(_) => {
                  // read hash digest and consume hasher
                  let result = hash_buffer(&buffer);
                  Ok(result)
              },
              Err(_) => {
                  eprintln!("Could not read file to end {:?}", path);
                  Err(String::from("Could not read file to end"))
              }
          }
      },
      Err(_) => {
        //   eprintln!("Could not open file {:?}", path);
          Err(String::from("Could not open file"))
      }
  }
}