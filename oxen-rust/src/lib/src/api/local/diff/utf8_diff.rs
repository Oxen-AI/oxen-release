use crate::error::OxenError;
use crate::model::diff::change_type::ChangeType;
use crate::model::diff::text_diff::LineDiff;
use crate::model::diff::text_diff::TextDiff;
use crate::util;

use difference::{Changeset, Difference};
use std::path::PathBuf;

pub fn diff(version_file_1: &PathBuf, version_file_2: &PathBuf) -> Result<TextDiff, OxenError> {
    let original_data = util::fs::read_from_path(version_file_1)?;
    let compare_data = util::fs::read_from_path(version_file_2)?;
    let Changeset { diffs, .. } = Changeset::new(&original_data, &compare_data, "\n");

    let mut result = TextDiff { lines: vec![] };
    for diff in diffs {
        match diff {
            Difference::Same(ref x) => {
                for split in x.split('\n') {
                    result.lines.push(LineDiff {
                        modification: ChangeType::Unchanged,
                        text: split.to_string(),
                    });
                }
            }
            Difference::Add(ref x) => {
                for split in x.split('\n') {
                    result.lines.push(LineDiff {
                        modification: ChangeType::Added,
                        text: split.to_string(),
                    });
                }
            }
            Difference::Rem(ref x) => {
                for split in x.split('\n') {
                    result.lines.push(LineDiff {
                        modification: ChangeType::Removed,
                        text: split.to_string(),
                    });
                }
            }
        }
    }

    Ok(result)
}
