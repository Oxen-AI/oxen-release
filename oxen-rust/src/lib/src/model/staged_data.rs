use colored::{ColoredString, Colorize};
use std::collections::HashMap;
use std::path::PathBuf;

use crate::model::{MergeConflict, StagedEntry, StagedEntryStatus, SummarizedStagedDirStats};

pub const MSG_CLEAN_REPO: &str = "nothing to commit, working tree clean\n";
pub const MSG_OXEN_ADD_FILE_EXAMPLE: &str =
    "  (use \"oxen add <file>...\" to update what will be committed)\n";
pub const MSG_OXEN_ADD_DIR_EXAMPLE: &str =
    "  (use \"oxen add <dir>...\" to update what will be committed)\n";
pub const MSG_OXEN_ADD_FILE_RESOLVE_CONFLICT: &str =
    "  (use \"oxen add <file>...\" to mark resolution)\n";
pub const MSG_OXEN_RESTORE_FILE: &str =
    "  (use \"oxen restore <file>...\" to discard changes in working directory)";

pub struct StagedData {
    pub added_dirs: SummarizedStagedDirStats,
    pub added_files: HashMap<PathBuf, StagedEntry>, // All the staged entries will be in here
    pub untracked_dirs: Vec<(PathBuf, usize)>,
    pub untracked_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
    pub removed_files: Vec<PathBuf>,
    pub merge_conflicts: Vec<MergeConflict>,
}

impl StagedData {
    pub fn empty() -> StagedData {
        StagedData {
            added_dirs: SummarizedStagedDirStats::new(),
            added_files: HashMap::new(),
            untracked_dirs: vec![],
            untracked_files: vec![],
            modified_files: vec![],
            removed_files: vec![],
            merge_conflicts: vec![],
        }
    }

    pub fn is_clean(&self) -> bool {
        self.added_dirs.is_empty()
            && self.added_files.is_empty()
            && self.untracked_files.is_empty()
            && self.untracked_dirs.is_empty()
            && self.modified_files.is_empty()
            && self.removed_files.is_empty()
            && self.merge_conflicts.is_empty()
    }

    pub fn has_added_entries(&self) -> bool {
        !self.added_dirs.is_empty() || !self.added_files.is_empty()
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

    /// Line by line output that we want to print
    ///
    /// # Arguments
    ///
    /// * `skip` - File index to skip printing for
    /// * `limit` - Max number of files to show
    /// * `print_all` - If true, ignores skip and limit and prints everything
    ///
    fn __collect_outputs(&self, skip: usize, limit: usize, print_all: bool) -> Vec<ColoredString> {
        let mut outputs: Vec<ColoredString> = vec![];

        if self.is_clean() {
            outputs.push(MSG_CLEAN_REPO.to_string().normal());
            return outputs;
        }

        self.__collect_added_dirs(&mut outputs, skip, limit, print_all);
        self.__collect_added_files(&mut outputs, skip, limit, print_all);
        self.__collect_modified_files(&mut outputs, skip, limit, print_all);
        self.__collect_merge_conflicts(&mut outputs, skip, limit, print_all);
        self.__collect_untracked_dirs(&mut outputs, skip, limit, print_all);
        self.__collect_untracked_files(&mut outputs, skip, limit, print_all);

        outputs
    }

    pub fn print_stdout(&self) {
        let skip: usize = 0;
        let limit: usize = 10;
        let print_all = false;
        let outputs = self.__collect_outputs(skip, limit, print_all);

        for output in outputs {
            print!("{}", output)
        }
    }

    pub fn print_stdout_with_params(&self, skip: usize, limit: usize, print_all: bool) {
        let outputs = self.__collect_outputs(skip, limit, print_all);

        for output in outputs {
            print!("{}", output)
        }
    }

    pub fn __collect_merge_conflicts(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
        if self.merge_conflicts.is_empty() {
            return;
        }

        outputs.push("Merge conflicts:".to_string().normal());
        outputs.push(format!("  {}", MSG_OXEN_ADD_FILE_RESOLVE_CONFLICT).normal());

        self.__collapse_outputs(
            &self.merge_conflicts,
            |conflict| {
                let path = &conflict.head_entry.path;

                // println!(
                //     "    LCA {} {:?}",
                //     conflict.lca_entry.commit_id,
                //     conflict.lca_entry.version_file()
                // );
                // println!(
                //     "    HEAD {} {:?}",
                //     conflict.head_entry.commit_id,
                //     conflict.head_entry.version_file()
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
            skip,
            limit,
            print_all,
        );
    }

    fn __collect_added_dirs(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
        let mut dirs: Vec<Vec<ColoredString>> = vec![];
        for (path, staged_dirs) in self.added_dirs.paths.iter() {
            let mut dir_row: Vec<ColoredString> = vec![];
            for staged_dir in staged_dirs.iter() {
                dir_row.push("  added: ".green());
                dir_row.push(staged_dir.path.to_str().unwrap().to_string().green().bold());

                let num_files_str = match staged_dir.num_files_staged {
                    1 => {
                        Some(format!(" with added {} file\n", staged_dir.num_files_staged).normal())
                    }
                    0 => {
                        // limit since we don't have any added files in this dir
                        log::warn!("Added dir with no files staged: {:?}", path);
                        None
                    }
                    _ => Some(
                        format!(" with added {} files\n", staged_dir.num_files_staged).normal(),
                    ),
                };
                if let Some(num_files_str) = num_files_str {
                    dir_row.push(num_files_str);
                } else {
                    dir_row.push("\n".normal());
                }
            }
            dirs.push(dir_row);
        }

        if dirs.is_empty() {
            return;
        }

        outputs.push("Directories to be committed\n".normal());
        self.__collapse_outputs(&dirs, |dir| dir.to_vec(), outputs, skip, limit, print_all);
    }

    fn __collect_added_files(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
        if self.added_files.is_empty() {
            return;
        }
        outputs.push("Files to be committed:\n".normal());

        let mut files_vec: Vec<(&PathBuf, &StagedEntry)> =
            self.added_files.iter().map(|(k, v)| (k, v)).collect();
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
            },
            outputs,
            skip,
            limit,
            print_all,
        );
    }

    fn __collect_modified_files(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
        if self.modified_files.is_empty() {
            // nothing to print
            return;
        }

        outputs.push("Modified files:".to_string().normal());
        outputs.push(format!("  {}", MSG_OXEN_ADD_FILE_EXAMPLE).normal());

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
            skip,
            limit,
            print_all,
        );
    }

    fn __collect_removed_files(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
        if self.removed_files.is_empty() {
            // nothing to print
            return;
        }

        outputs.push("Removed files:".to_string().normal());
        outputs.push(format!("  {}", MSG_OXEN_ADD_FILE_EXAMPLE).normal());

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
            skip,
            limit,
            print_all,
        );
    }

    fn __collect_untracked_dirs(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
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
                skip,
                limit,
                print_all,
            )
        }
    }

    fn __collect_untracked_files(
        &self,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) {
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
                skip,
                limit,
                print_all,
            )
        }
    }

    fn __collapse_outputs<T, F>(
        &self,
        inputs: &Vec<T>,
        to_components: F,
        outputs: &mut Vec<ColoredString>,
        skip: usize,
        limit: usize,
        print_all: bool,
    ) where
        F: Fn(&T) -> Vec<ColoredString>,
    {
        if inputs.is_empty() {
            return;
        }

        let total = skip + limit;
        for (i, input) in inputs.iter().enumerate() {
            if i < skip && !print_all {
                continue;
            }
            if i >= total && !print_all {
                break;
            }
            let mut components = to_components(input);
            outputs.append(&mut components);
        }

        log::debug!("__collapse_outputs {} > {}", inputs.len(), total);
        if inputs.len() > limit && !print_all {
            let remaining = inputs.len() - limit;
            outputs.push(format!("  ... and {} others\n", remaining).normal());
        }

        outputs.push("\n".normal());
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
        MSG_CLEAN_REPO, MSG_OXEN_ADD_DIR_EXAMPLE, MSG_OXEN_ADD_FILE_EXAMPLE,
    };
    use crate::model::StagedEntryStatus;
    use crate::model::{StagedData, StagedEntry};

    #[test]
    fn test_staged_data_collect_clean_repo() {
        let staged_data = StagedData::empty();

        let outputs = staged_data.__collect_outputs(0, 10, false);
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].to_string(), MSG_CLEAN_REPO);
    }

    #[test]
    fn test_staged_data_collect_added_files() {
        let mut staged_data = StagedData::empty();
        staged_data.added_files.insert(
            PathBuf::from("file_1.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_2.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_3.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_4.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_5.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );

        let num_to_print = 3;
        let outputs = staged_data.__collect_outputs(0, num_to_print, false);
        assert_eq!(outputs[0], "Files to be committed:\n".normal());
        assert_eq!(outputs[1], "  new file: ".green());
        assert_eq!(outputs[2], "file_1.jpg\n".green().bold());
        assert_eq!(outputs[3], "  new file: ".green());
        assert_eq!(outputs[4], "file_2.jpg\n".green().bold());
        assert_eq!(outputs[5], "  new file: ".green());
        assert_eq!(outputs[6], "file_3.jpg\n".green().bold());
        assert_eq!(outputs[7], "  ... and 2 others\n".normal());
    }

    #[test]
    fn test_staged_data_collect_added_files_length() {
        let mut staged_data = StagedData::empty();
        staged_data.added_files.insert(
            PathBuf::from("file_1.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_2.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_3.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_4.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );
        staged_data.added_files.insert(
            PathBuf::from("file_5.jpg"),
            StagedEntry::empty_status(StagedEntryStatus::Added),
        );

        let num_to_print = 3;
        let outputs = staged_data.__collect_outputs(2, num_to_print, false);
        assert_eq!(outputs[0], "Files to be committed:\n".normal());
        assert_eq!(outputs[1], "  new file: ".green());
        assert_eq!(outputs[2], "file_3.jpg\n".green().bold());
        assert_eq!(outputs[3], "  new file: ".green());
        assert_eq!(outputs[4], "file_4.jpg\n".green().bold());
        assert_eq!(outputs[5], "  new file: ".green());
        assert_eq!(outputs[6], "file_5.jpg\n".green().bold());
        assert_eq!(outputs[7], "  ... and 2 others\n".normal());
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

        let num_to_print = 3;
        let outputs = staged_data.__collect_outputs(0, num_to_print, false);
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

        let outputs = staged_data.__collect_outputs(0, 0, true);
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

        let num_to_print = 3;
        let outputs = staged_data.__collect_outputs(0, num_to_print, false);
        assert_eq!(outputs[0], "Untracked Directories\n".normal());
        assert_eq!(outputs[1], MSG_OXEN_ADD_DIR_EXAMPLE.normal());
        assert_eq!(outputs[2], "  annotations/ ".red().bold());
        assert_eq!(outputs[3], "(1 item)\n".normal());
        assert_eq!(outputs[4], "  test/        ".red().bold());
        assert_eq!(outputs[5], "(4 items)\n".normal());
        assert_eq!(outputs[6], "  train/       ".red().bold());
        assert_eq!(outputs[7], "(10 items)\n".normal());
    }
}
