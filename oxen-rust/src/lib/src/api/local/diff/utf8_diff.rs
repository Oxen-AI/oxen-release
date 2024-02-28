use crate::error::OxenError;
use crate::util;

use colored::Colorize;
use difference::{Changeset, Difference};
use std::path::PathBuf;

pub fn compare(version_file_1: &PathBuf, version_file_2: &PathBuf) -> Result<String, OxenError> {
    let original_data = util::fs::read_from_path(version_file_1)?;
    let compare_data = util::fs::read_from_path(version_file_2)?;
    let Changeset { diffs, .. } = Changeset::new(&original_data, &compare_data, "\n");

    let mut outputs: Vec<String> = vec![];
    for diff in diffs {
        match diff {
            Difference::Same(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!(" {split}\n").normal().to_string());
                }
            }
            Difference::Add(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("+{split}\n").green().to_string());
                }
            }
            Difference::Rem(ref x) => {
                for split in x.split('\n') {
                    outputs.push(format!("-{split}\n").red().to_string());
                }
            }
        }
    }

    Ok(outputs.join(""))
}
