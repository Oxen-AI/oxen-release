use colored::Colorize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};

use crate::model::{MergeConflict, StagedEntry, StagedEntryStatus, SummarizedStagedDirStats};
use crate::util;

pub struct StagedData {
    pub added_dirs: SummarizedStagedDirStats,
    // Would it be easier to have filepath in staged entry here...? and we don't need to collapse for output anymore...
    // ALSO - this should just be the added files at the status *top* level, and the total will be
    //        added_dirs.total + added_files.len()
    // I think this makes sense...and will fix our modified tests, because we are not committing the files in the top level
    // we are not committing the files in the top level, because we were iterating over added_dirs and not added_files
    // whew.
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

    pub fn print(&self) {
        if self.is_clean() {
            println!("nothing to commit, working tree clean");
            return;
        }

        // List added files
        if self.has_added_entries() {
            self.print_added();
        }

        if self.has_modified_entries() {
            self.print_modified();
        }

        if self.has_merge_conflicts() {
            self.print_merge_conflicts();
        }

        if self.has_removed_entries() {
            self.print_removed();
        }

        if self.has_untracked_entries() {
            self.print_untracked();
        }
    }

    pub fn print_added(&self) {
        println!("Changes to be committed:");
        self.print_added_dirs();
        self.print_added_files();
        println!();
    }

    pub fn print_untracked(&self) {
        println!("Untracked:");
        println!("  (use \"oxen add <file>...\" to update what will be committed)");
        self.print_untracked_dirs();
        self.print_untracked_files();
        println!();
    }

    pub fn print_modified(&self) {
        println!("Modified files:");
        println!("  (use \"oxen add <file>...\" to update what will be committed)");
        self.print_modified_files();
        println!();
    }

    pub fn print_merge_conflicts(&self) {
        println!("Unmerged paths:");
        println!("  (use \"oxen add <file>...\" to mark resolution)");
        for conflict in self.merge_conflicts.iter() {
            let path = &conflict.head_entry.path;
            let added_file_str = format!("  both modified:  {}", path.to_str().unwrap()).red();
            println!("{}", added_file_str);
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
        }
        println!();
    }

    pub fn print_removed(&self) {
        println!("Removed files:");
        println!("  (use \"oxen add <file>...\" to update what will be committed)");
        println!("  (use \"oxen restore <file>...\" to discard changes in working directory)");
        self.print_removed_files();
        println!();
    }

    fn print_added_dirs(&self) {
        println!("Directories:");
        for (path, staged_dirs) in self.added_dirs.paths.iter() {
            for staged_dir in staged_dirs.iter() {
                let added_file_str = format!("  added:  {}/", path.to_str().unwrap()).green();
                let num_files_str = match staged_dir.num_files_staged {
                    1 => Some(format!("with added {} file\n", staged_dir.num_files_staged)),
                    0 => {
                        // Skip since we don't have any added files in this dir
                        log::warn!("Added dir with no files staged: {:?}", path);
                        None
                    }
                    _ => Some(format!(
                        "with added {} files\n",
                        staged_dir.num_files_staged
                    )),
                };
                if let Some(num_files_str) = num_files_str {
                    print!("{} {}", added_file_str, num_files_str);
                }
            }
        }
    }

    fn print_added_files(&self) {
        println!("Files:");
        let current_dir = env::current_dir().unwrap();
        let repo_path = util::fs::get_repo_root(&current_dir);
        if repo_path.is_none() {
            eprintln!("Err: print_removed_files() Could not find oxen repository");
            return;
        }

        for (short_path, entry) in self.added_files.iter() {
            log::debug!("{:?} -> {:?}", short_path, entry);
            match entry.status {
                StagedEntryStatus::Removed => {
                    let print_str =
                        format!("  removed:  {}", short_path.to_str().unwrap()).green();
                    println!("{}", print_str);
                }
                StagedEntryStatus::Modified => {
                    let print_str =
                        format!("  modified:  {}", short_path.to_str().unwrap()).green();
                    println!("{}", print_str);
                }
                StagedEntryStatus::Added => {
                    let print_str =
                        format!("  added:  {}", short_path.to_str().unwrap()).green();
                    println!("{}", print_str);
                }
            }
        }
    }

    fn print_modified_files(&self) {
        for file in self.modified_files.iter() {
            let print_str = format!("  modified:  {}", file.to_str().unwrap()).yellow();
            println!("{}", print_str);
        }
    }

    fn get_top_level_removed_counts(
        &self,
        repo_path: &Path,
        paths: &[PathBuf],
    ) -> HashMap<PathBuf, usize> {
        let mut top_level_counts: HashMap<PathBuf, usize> = HashMap::new();
        for short_path in paths.iter() {
            let full_path = repo_path.join(short_path);

            let path = self.get_top_level_dir(repo_path, &full_path);
            if !top_level_counts.contains_key(&path) {
                top_level_counts.insert(path.to_path_buf(), 0);
            }
            *top_level_counts.get_mut(&path).unwrap() += 1;
        }
        top_level_counts
    }

    fn get_remaining_removed_counts(
        &self,
        top_level_counts: &mut HashMap<PathBuf, usize>,
        repo_path: &Path,
    ) -> HashMap<PathBuf, usize> {
        let mut remaining_file_count: HashMap<PathBuf, usize> = HashMap::new();
        for (dir, _) in top_level_counts.iter() {
            let full_path = repo_path.join(dir);

            let count = util::fs::rcount_files_in_dir(&full_path);
            remaining_file_count.insert(dir.to_owned(), count);
        }
        remaining_file_count
    }

    fn print_removed_files(&self) {
        if self.removed_files.is_empty() {
            // nothing to print
            return;
        }

        let current_dir = env::current_dir().unwrap();
        let repo_path = util::fs::get_repo_root(&current_dir);
        if repo_path.is_none() {
            eprintln!("Err: print_removed_files() Could not find oxen repository");
            return;
        }

        // Unwrap because is some
        let repo_path = repo_path.unwrap();
        // println!("Got repo path {:?} {:?}", current_dir, repo_path);

        // Get the top level dirs so that we don't have to print every file
        let mut top_level_counts =
            self.get_top_level_removed_counts(&repo_path, &self.removed_files);

        // See the actual counts in the dir, if nothing remains, we can just print the top level summary
        let remaining_file_count =
            self.get_remaining_removed_counts(&mut top_level_counts, &repo_path);

        // When iterating, if remaining_file_count[p] == 0 or we have more than N entries then we only print the count
        let mut summarized: HashSet<PathBuf> = HashSet::new();
        for short_path in self.removed_files.iter() {
            let full_path = repo_path.join(short_path);
            let path = self.get_top_level_dir(&repo_path, &full_path);

            let count = top_level_counts[&path];
            if (0 == remaining_file_count[&path] || top_level_counts[&path] > 5)
                && !summarized.contains(&path)
            {
                let added_file_str = format!(
                    "  removed: {}\n    which had {} files including {}",
                    path.to_str().unwrap(),
                    count,
                    short_path.to_str().unwrap()
                )
                .red();
                println!("{}", added_file_str);

                summarized.insert(path.to_owned());
            }

            if !summarized.contains(&path) {
                let added_file_str = format!("  removed:  {}", short_path.to_str().unwrap()).red();
                println!("{}", added_file_str);
            }
        }
    }

    fn get_top_level_dir(&self, repo_path: &Path, full_path: &Path) -> PathBuf {
        let mut mut_path = full_path.to_path_buf();
        let mut components: Vec<PathBuf> = vec![];
        while let Some(parent) = mut_path.parent() {
            // println!("get_top_level_dir GOT PARENT {:?}", parent);
            if repo_path == parent {
                break;
            }

            if let Some(filename) = parent.file_name() {
                // println!("get_top_level_dir filename {:?}", filename);
                components.push(PathBuf::from(filename));
            }

            mut_path.pop();
        }
        components.reverse();

        let mut result = PathBuf::new();
        for component in components.iter() {
            result = result.join(component);
        }

        // println!("get_top_level_dir got result {:?}", result);

        result
    }

    fn print_untracked_dirs(&self) {
        // List untracked directories
        // We are ignoring the count because it is too computationally expensive to compute for some dirs
        println!("Directories:");
        for (dir, _count) in self.untracked_dirs.iter() {
            // Make sure we can grab the filename
            if let Some(filename) = dir.file_name() {
                let print_str = format!("  {}/", filename.to_str().unwrap()).red();
                println!("{}", print_str);
            }
        }
    }

    fn print_untracked_files(&self) {
        // List untracked files
        println!("Files:");
        for file in self.untracked_files.iter() {
            let print_str = file.to_str().unwrap().to_string().red();
            println!("  {}", print_str);
        }
        println!();
    }
}
