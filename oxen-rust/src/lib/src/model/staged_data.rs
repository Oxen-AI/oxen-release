use colored::{ColoredString, Colorize};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

use crate::model::{MergeConflict, StagedEntry, StagedEntryStatus, SummarizedStagedDirStats};

use super::Schema;

pub const MSG_CLEAN_REPO: &str = "nothing to commit, working tree clean\n";
pub const MSG_OXEN_ADD_FILE_EXAMPLE: &str =
    "  (use \"oxen add <file>...\" to update what will be committed)\n";
pub const MSG_OXEN_RM_FILE_EXAMPLE: &str =
    "  (use \"oxen rm <file>...\" to update what will be committed)\n";
pub const MSG_OXEN_ADD_DIR_EXAMPLE: &str =
    "  (use \"oxen add <dir>...\" to update what will be committed)\n";
pub const MSG_OXEN_ADD_FILE_RESOLVE_CONFLICT: &str =
    "  (use \"oxen add <file>...\" to mark resolution)\n";
pub const MSG_OXEN_RESTORE_FILE: &str =
    "  (use \"oxen restore <file>...\" to discard changes in working directory)";
pub const MSG_OXEN_RESTORE_STAGED_FILE: &str =
    "  (use \"oxen restore --staged <file> ...\" to unstage)\n";
pub const MSG_OXEN_SHOW_SCHEMA_STAGED: &str =
    "  (use \"oxen schemas --staged <PATH_OR_HASH>\" to view staged schema)\n";

#[derive(Debug, Clone)]
pub struct StagedDataOpts {
    pub skip: usize,
    pub limit: usize,
    pub print_all: bool,
    pub is_remote: bool,
}

impl Default for StagedDataOpts {
    fn default() -> StagedDataOpts {
        StagedDataOpts {
            skip: 0,
            limit: 10,
            print_all: false,
            is_remote: false,
        }
    }
}

impl fmt::Display for StagedData {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let opts = StagedDataOpts::default();
        let outputs = self.__collect_outputs(&opts);

        for output in outputs {
            write!(f, "{}", output)?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct StagedData {
    pub staged_dirs: SummarizedStagedDirStats,
    pub staged_files: HashMap<PathBuf, StagedEntry>, // All the staged entries will be in here
    pub staged_schemas: HashMap<PathBuf, Schema>,    // All the staged entries will be in here
    pub untracked_dirs: Vec<(PathBuf, usize)>,
    pub untracked_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
    pub moved_files: Vec<(PathBuf, PathBuf, String)>,
    pub removed_files: Vec<PathBuf>,
    //TODONOW drop temp_removed_files everywhere
    pub merge_conflicts: Vec<MergeConflict>,
}

impl StagedData {
    pub fn empty() -> StagedData {
        StagedData {
            staged_dirs: SummarizedStagedDirStats::new(),
            staged_files: HashMap::new(),
            staged_schemas: HashMap::new(),
            untracked_dirs: vec![],
            untracked_files: vec![],
            modified_files: vec![],
            removed_files: vec![],
            moved_files: vec![],
            merge_conflicts: vec![],
        }
    }

    pub fn is_clean(&self) -> bool {
        self.staged_files.is_empty()
            && self.staged_schemas.is_empty()
            && self.untracked_files.is_empty()
            && self.untracked_dirs.is_empty()
            && self.modified_files.is_empty()
            && self.removed_files.is_empty()
            && self.merge_conflicts.is_empty()
            && self.moved_files.is_empty() // TODONOW consider `moved_entries`
    }

    pub fn has_added_entries(&self) -> bool {
        !self.staged_dirs.is_empty() || !self.staged_files.is_empty()
    }

    pub fn has_modified_entries(&self) -> bool {
        !self.modified_files.is_empty()
    }

    pub fn has_removed_entries(&self) -> bool {
        !self.removed_files.is_empty()
    }

    pub fn has_untracked_entries(&self) -> bool {
        !self.untracked_dirs.is_empty() || !self.untracked_files.is_empty()
    }

    pub fn has_merge_conflicts(&self) -> bool {
        !self.merge_conflicts.is_empty()
    }

    pub fn has_moved_entries(&self) -> bool {
        !self.moved_files.is_empty()
    }

    /// Line by line output that we want to print
    ///
    /// # Arguments
    ///
    /// * `skip` - File index to skip printing for
    /// * `limit` - Max number of files to show
    /// * `print_all` - If true, ignores skip and limit and prints everything
    ///
    fn __collect_outputs(&self, opts: &StagedDataOpts) -> Vec<ColoredString> {
        let mut outputs: Vec<ColoredString> = vec![];

        if self.is_clean() {
            outputs.push(MSG_CLEAN_REPO.to_string().normal());
            return outputs;
        }

        self.staged_dirs(&mut outputs, opts);
        self.staged_files(&mut outputs, opts);
        self.staged_schemas(&mut outputs, opts);
        self.__collect_modified_files(&mut outputs, opts);
        self.__collect_merge_conflicts(&mut outputs, opts);
        self.__collect_untracked_dirs(&mut outputs, opts);
        self.__collect_untracked_files(&mut outputs, opts);
        self.__collect_removed_files(&mut outputs, opts);

        outputs
    }

    pub fn print_stdout(&self) {
        let opts = StagedDataOpts::default();
        let outputs = self.__collect_outputs(&opts);

        for output in outputs {
            print!("{output}")
        }
    }

    pub fn print_stdout_with_params(&self, opts: &StagedDataOpts) {
        let outputs = self.__collect_outputs(opts);

        for output in outputs {
            print!("{output}")
        }
    }

    pub fn __collect_merge_conflicts(
        &self,
        outputs: &mut Vec<ColoredString>,
        opts: &StagedDataOpts,
    ) {
        if self.merge_conflicts.is_empty() {
            return;
        }

        outputs.push("Merge conflicts:".to_string().normal());
        outputs.push(format!("  {MSG_OXEN_ADD_FILE_RESOLVE_CONFLICT}").normal());

        self.__collapse_outputs(
            &self.merge_conflicts,
            |conflict| {
                let path = &conflict.base_entry.path;

                // println!(
                //     "    LCA {} {:?}",
                //     conflict.lca_entry.commit_id,
                //     conflict.lca_entry.version_file()
                // );
                // println!(
                //     "    HEAD {} {:?}",
                //     conflict.base_entry.commit_id,
                //     conflict.base_entry.version_file()
                // );
                // println!(
                //     "    MERGE {} {:?}",
                //     conflict.merge_entry.commit_id,
                //     conflict.merge_entry.version_file()
                // );

                vec![
                    "  both modified: ".to_string().yellow(),
                    format!("{}\n", path.to_str().unwrap()).yellow().bold(),
                ]
            },
            outputs,
            opts,
        );
        outputs.push("\n".normal());
    }

    fn staged_dirs(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        let mut dirs: Vec<Vec<ColoredString>> = vec![];
        for (path, staged_dirs) in self.staged_dirs.paths.iter() {
            let mut dir_row: Vec<ColoredString> = vec![];
            for staged_dir in staged_dirs.iter() {
                if staged_dir.num_files_staged == 0 {
                    continue;
                }

                match staged_dir.status {
                    StagedEntryStatus::Added => {
                        dir_row.push("  added: ".green());
                    }
                    StagedEntryStatus::Modified => {
                        dir_row.push("  modified: ".green());
                    }
                    StagedEntryStatus::Removed => {
                        dir_row.push("  removed: ".green());
                    }
                    StagedEntryStatus::Moved => {
                        dir_row.push("  moved: ".green());
                    }
                }

                dir_row.push(staged_dir.path.to_str().unwrap().to_string().green().bold());

                let num_files_str = match staged_dir.num_files_staged {
                    1 => Some(format!(" with {} file\n", staged_dir.num_files_staged).normal()),
                    0 => {
                        // limit since we don't have any staged files in this dir
                        log::warn!("Added dir with no files staged: {:?}", path);
                        None
                    }
                    _ => Some(format!(" with {} files\n", staged_dir.num_files_staged).normal()),
                };
                if let Some(num_files_str) = num_files_str {
                    dir_row.push(num_files_str);
                } else {
                    dir_row.push("\n".normal());
                }
            }
            if !dir_row.is_empty() {
                dirs.push(dir_row);
            }
        }

        if dirs.is_empty() {
            return;
        }

        outputs.push("Directories to be committed\n".normal());
        self.__collapse_outputs(&dirs, |dir| dir.to_vec(), outputs, opts);
        outputs.push("\n".normal());
    }

    fn staged_files(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        if self.staged_files.is_empty() {
            return;
        }
        outputs.push("Files to be committed:\n".normal());
        if (!self.staged_files.is_empty() || !self.staged_dirs.is_empty()) && !opts.is_remote {
            outputs.push(MSG_OXEN_RESTORE_STAGED_FILE.normal())
        }

        // TODONOW: delete

        let files = &self.staged_files;
        // // For each added, look for a removed with the same hash. If there is a match, delete both entries and replace them with a moved entry
        let mut files_vec: Vec<(&PathBuf, &StagedEntry)> =
            files.iter().map(|(k, v)| (k, v)).collect();

        // let mut added_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();
        // let mut removed_map: HashMap<String, Vec<&PathBuf>> = HashMap::new();

        // let mut moved_entries: Vec<(&PathBuf, &PathBuf, String)> = vec![];
        // for (path, entry) in files_vec.iter() {
        //     match entry.status {
        //         StagedEntryStatus::Added => {
        //             added_map
        //                 .entry(entry.hash.clone())
        //                 .or_insert_with(Vec::new)
        //                 .push(path);
        //         }
        //         StagedEntryStatus::Removed => {
        //             removed_map
        //                 .entry(entry.hash.clone())
        //                 .or_insert_with(Vec::new)
        //                 .push(path);
        //         }
        //         _ => continue,
        //     }
        // }

        // for (hash, added_paths) in added_map.iter_mut() {
        //     if let Some(removed_paths) = removed_map.get_mut(hash) {
        //         while !added_paths.is_empty() && !removed_paths.is_empty() {
        //             if let (Some(added_path), Some(removed_path)) = (added_paths.pop(), removed_paths.pop()) {
        //                 moved_entries.push((added_path, removed_path, hash.to_string()));
        //             }
        //         }
        //     }
        // }
        // Remove entries (for display purposes only)
        // TODONOW fix this logic
        let mut moved_staged_entries: Vec<StagedEntry> = Vec::new();

        // self.moved_files = moved_entries.clone().into_iter().map(|(from, to, _)| (from.clone(), to.clone())).collect();

        for (path, removed_path, hash) in self.moved_files.iter() {
            files_vec.retain(|(p, _)| p != &path && p != &removed_path);

            let moved_entry = StagedEntry {
                hash: hash.to_string(),
                status: StagedEntryStatus::Moved,
            };
            moved_staged_entries.push(moved_entry.clone());
            // let last_entry_ref = moved_staged_entries.last().unwrap(); // Safe due to the push just above
            // files_vec.push((path, last_entry_ref));
        }
        // Old code
        // let mut files_vec: Vec<(&PathBuf, &StagedEntry)> =
        //     self.staged_files.iter().map(|(k, v)| (k, v)).collect();
        files_vec.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        self.__collapse_outputs(
            &files_vec,
            |(path, entry)| match entry.status {
                StagedEntryStatus::Removed => {
                    vec![
                        "  removed: ".green(),
                        format!("{}\n", path.to_str().unwrap()).green().bold(),
                    ]
                }
                StagedEntryStatus::Modified => {
                    vec![
                        "  modified: ".green(),
                        format!("{}\n", path.to_str().unwrap()).green().bold(),
                    ]
                }
                StagedEntryStatus::Added => {
                    vec![
                        "  new file: ".green(),
                        format!("{}\n", path.to_str().unwrap()).green().bold(),
                    ]
                }
                StagedEntryStatus::Moved => {
                    vec![
                        "  moved: ".green(),
                        format!("{}\n", path.to_str().unwrap()).green().bold(),
                    ]
                }
            },
            outputs,
            opts,
        );

        // TODONOW get a copy of, and sort, these mvoed_files
        // Sort moved_entries
        // self.moved_files.sort_by(|(a, _, _), (b, _, _)| a.partial_cmp(b).unwrap());
        self.__collapse_outputs(
            &self.moved_files,
            |(path, removed_path, _hash)| {
                vec![
                    "  moved: ".green(),
                    format!(
                        "{} -> {}\n",
                        removed_path.to_str().unwrap(),
                        path.to_str().unwrap()
                    )
                    .green()
                    .bold(),
                ]
            },
            outputs,
            opts,
        );

        // Add moved entries

        // TODO: Can this be more generic?
        let total = self.staged_dirs.num_files_staged;
        if opts.is_remote && total > opts.limit {
            let remaining = total - opts.limit;
            outputs.push(format!("  ... and {remaining} others\n").normal());
        }
        outputs.push("\n".normal());
    }

    fn staged_schemas(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        if self.staged_schemas.is_empty() {
            return;
        }
        outputs.push("Schemas to be committed\n".normal());
        outputs.push(MSG_OXEN_SHOW_SCHEMA_STAGED.normal());

        let mut files_vec: Vec<(&PathBuf, &Schema)> =
            self.staged_schemas.iter().map(|(k, v)| (k, v)).collect();
        files_vec.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());
        self.__collapse_outputs(
            &files_vec,
            |(path, schema)| {
                let schema_ref = if let Some(name) = &schema.name {
                    name
                } else {
                    &schema.hash
                };
                vec![
                    "  detected schema: ".green(),
                    format!("{} {}\n", path.to_str().unwrap(), schema_ref)
                        .green()
                        .bold(),
                ]
            },
            outputs,
            opts,
        );
    }

    fn __collect_modified_files(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        if self.modified_files.is_empty() {
            // nothing to print
            return;
        }

        outputs.push("Modified files:".to_string().normal());

        if opts.is_remote {
            outputs.push("\n".to_string().normal());
        } else {
            outputs.push(format!("  {MSG_OXEN_ADD_FILE_EXAMPLE}").normal());
        }

        let mut files = self.modified_files.clone();
        files.sort();

        self.__collapse_outputs(
            &files,
            |file| {
                vec![
                    "  modified: ".to_string().yellow(),
                    format!("{}\n", file.to_str().unwrap()).yellow().bold(),
                ]
            },
            outputs,
            opts,
        );
        outputs.push("\n".normal());
    }

    fn __collect_removed_files(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        if self.removed_files.is_empty() {
            // nothing to print
            return;
        }

        outputs.push("Removed Files\n".to_string().normal());
        outputs.push(MSG_OXEN_RM_FILE_EXAMPLE.to_string().normal());

        let mut files = self.removed_files.clone();
        files.sort();

        self.__collapse_outputs(
            &files,
            |file| {
                vec![
                    "  removed: ".to_string().red(),
                    format!("{}\n", file.to_str().unwrap()).red().bold(),
                ]
            },
            outputs,
            opts,
        );
        outputs.push("\n".normal());
    }

    fn __collect_untracked_dirs(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        // List untracked files
        if !self.untracked_dirs.is_empty() {
            outputs.push("Untracked Directories\n".normal());
            outputs.push(MSG_OXEN_ADD_DIR_EXAMPLE.normal());

            let mut dirs = self.untracked_dirs.clone();
            dirs.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap());

            let max_dir_len = dirs
                .iter()
                .map(|(path, _size)| path.to_str().unwrap().len())
                .max()
                .unwrap();

            self.__collapse_outputs(
                &dirs,
                |(path, size)| {
                    let path_str = path.to_str().unwrap();
                    let num_spaces = max_dir_len - path_str.len();
                    vec![
                        format!("  {}/ {}", path_str, StagedData::spaces(num_spaces))
                            .red()
                            .bold(),
                        format!("({} {})\n", size, StagedData::item_str_plural(*size)).normal(),
                    ]
                },
                outputs,
                opts,
            );
            outputs.push("\n".normal());
        }
    }

    fn __collect_untracked_files(&self, outputs: &mut Vec<ColoredString>, opts: &StagedDataOpts) {
        // List untracked files
        if !self.untracked_files.is_empty() {
            outputs.push("Untracked Files\n".normal());
            outputs.push(MSG_OXEN_ADD_FILE_EXAMPLE.normal());

            let mut files = self.untracked_files.clone();
            files.sort();

            self.__collapse_outputs(
                &files,
                |f| vec![format!("  {}\n", f.to_str().unwrap()).red().bold()],
                outputs,
                opts,
            );
            outputs.push("\n".normal());
        }
    }

    fn __collapse_outputs<T, F>(
        &self,
        inputs: &Vec<T>,
        to_components: F,
        outputs: &mut Vec<ColoredString>,
        opts: &StagedDataOpts,
    ) where
        F: Fn(&T) -> Vec<ColoredString>,
    {
        if inputs.is_empty() {
            return;
        }

        let total = opts.skip + opts.limit;
        for (i, input) in inputs.iter().enumerate() {
            if i < opts.skip && !opts.print_all {
                continue;
            }
            if i >= total && !opts.print_all {
                break;
            }
            let mut components = to_components(input);
            outputs.append(&mut components);
        }

        if inputs.len() > opts.limit && !opts.print_all {
            let remaining = inputs.len() - opts.limit;
            outputs.push(format!("  ... and {remaining} others\n").normal());
        }
    }

    pub fn item_str_plural(n: usize) -> String {
        if n == 1 {
            String::from("item")
        } else {
            String::from("items")
        }
    }

    pub fn spaces(n: usize) -> String {
        let mut ret = String::from("");
        for _ in 0..n {
            ret.push(' ');
        }
        ret
    }
}

#[cfg(test)]
mod tests {

    use colored::Colorize;
    use std::path::PathBuf;

    use crate::model::staged_data::{
        StagedDataOpts, MSG_CLEAN_REPO, MSG_OXEN_ADD_DIR_EXAMPLE, MSG_OXEN_ADD_FILE_EXAMPLE,
        MSG_OXEN_RESTORE_STAGED_FILE, MSG_OXEN_RM_FILE_EXAMPLE,
    };
    use crate::model::StagedEntryStatus;
    use crate::model::{StagedData, StagedEntry};

    #[test]
    fn test_staged_data_collect_clean_repo() {
        let staged_data = StagedData::empty();
        let opts = StagedDataOpts::default();
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].to_string(), MSG_CLEAN_REPO);
    }

    #[test]
    fn staged_files() {
        let mut staged_data = StagedData::empty();
        staged_data.staged_files.insert(
            PathBuf::from("file_1.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_2.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_3.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_4.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_5.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );

        let opts = StagedDataOpts {
            limit: 3,
            ..StagedDataOpts::default()
        };
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Files to be committed:\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_RESTORE_STAGED_FILE.normal());
        assert_eq!(outputs[2], "  new file: ".green());
        assert_eq!(outputs[3], "file_1.jpg\n".green().bold());
        assert_eq!(outputs[4], "  new file: ".green());
        assert_eq!(outputs[5], "file_2.jpg\n".green().bold());
        assert_eq!(outputs[6], "  new file: ".green());
        assert_eq!(outputs[7], "file_3.jpg\n".green().bold());
        assert_eq!(outputs[8], "  ... and 2 others\n".normal());
    }

    #[test]
    fn staged_files_length() {
        let mut staged_data = StagedData::empty();
        staged_data.staged_files.insert(
            PathBuf::from("file_1.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_2.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_3.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_4.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.staged_files.insert(
            PathBuf::from("file_5.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );

        let opts = StagedDataOpts {
            limit: 3,
            skip: 2,
            ..StagedDataOpts::default()
        };
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Files to be committed:\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_RESTORE_STAGED_FILE.normal());
        assert_eq!(outputs[2], "  new file: ".green());
        assert_eq!(outputs[3], "file_3.jpg\n".green().bold());
        assert_eq!(outputs[4], "  new file: ".green());
        assert_eq!(outputs[5], "file_4.jpg\n".green().bold());
        assert_eq!(outputs[6], "  new file: ".green());
        assert_eq!(outputs[7], "file_5.jpg\n".green().bold());
        assert_eq!(outputs[8], "  ... and 2 others\n".normal());
    }

    #[test]
    fn test_staged_data_collect_untracked_files() {
        let mut staged_data = StagedData::empty();
        staged_data
            .untracked_files
            .push(PathBuf::from("file_1.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_2.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_3.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_4.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_5.jpg"));

        let opts = StagedDataOpts {
            limit: 3,
            ..StagedDataOpts::default()
        };
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Untracked Files\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_ADD_FILE_EXAMPLE.normal());
        assert_eq!(outputs[2], "  file_1.jpg\n".red().bold());
        assert_eq!(outputs[3], "  file_2.jpg\n".red().bold());
        assert_eq!(outputs[4], "  file_3.jpg\n".red().bold());
        assert_eq!(outputs[5], "  ... and 2 others\n".normal());
    }

    #[test]
    fn test_staged_data_collect_untracked_files_print_all() {
        let mut staged_data = StagedData::empty();
        staged_data
            .untracked_files
            .push(PathBuf::from("file_1.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_2.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_3.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_4.jpg"));
        staged_data
            .untracked_files
            .push(PathBuf::from("file_5.jpg"));

        let opts = StagedDataOpts {
            print_all: true,
            ..StagedDataOpts::default()
        };
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Untracked Files\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_ADD_FILE_EXAMPLE.normal());
        assert_eq!(outputs[2], "  file_1.jpg\n".red().bold());
        assert_eq!(outputs[3], "  file_2.jpg\n".red().bold());
        assert_eq!(outputs[4], "  file_3.jpg\n".red().bold());
        assert_eq!(outputs[5], "  file_4.jpg\n".red().bold());
        assert_eq!(outputs[6], "  file_5.jpg\n".red().bold());
    }

    #[test]
    fn test_staged_data_collect_untracked_dirs() {
        let mut staged_data = StagedData::empty();
        staged_data
            .untracked_dirs
            .push((PathBuf::from("train"), 10));
        staged_data.untracked_dirs.push((PathBuf::from("test"), 4));
        staged_data
            .untracked_dirs
            .push((PathBuf::from("annotations"), 1));

        let opts = StagedDataOpts {
            limit: 3,
            ..StagedDataOpts::default()
        };
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Untracked Directories\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_ADD_DIR_EXAMPLE.normal());
        assert_eq!(outputs[2], "  annotations/ ".red().bold());
        assert_eq!(outputs[3], "(1 item)\n".normal());
        assert_eq!(outputs[4], "  test/        ".red().bold());
        assert_eq!(outputs[5], "(4 items)\n".normal());
        assert_eq!(outputs[6], "  train/       ".red().bold());
        assert_eq!(outputs[7], "(10 items)\n".normal());
    }

    #[test]
    fn test_staged_data_remove_file() {
        let mut staged_data = StagedData::empty();
        staged_data.removed_files.push(PathBuf::from("README.md"));

        let opts = StagedDataOpts::default();
        let outputs = staged_data.__collect_outputs(&opts);
        assert_eq!(outputs[0], "Removed Files\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_RM_FILE_EXAMPLE.normal());
        assert_eq!(outputs[2], "  removed: ".red());
        assert_eq!(outputs[3], "README.md\n".red().bold());
    }
}
