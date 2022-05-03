use colored::Colorize;
use std::collections::{HashMap, HashSet};
use std::env;
use std::path::{Path, PathBuf};

use crate::model::{StagedEntry, StagedEntryStatus};
use crate::util;

pub struct StagedData {
    pub added_dirs: Vec<(PathBuf, usize)>,
    pub added_files: Vec<(PathBuf, StagedEntry)>,
    pub untracked_dirs: Vec<(PathBuf, usize)>,
    pub untracked_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
    pub removed_files: Vec<PathBuf>,
}

impl StagedData {
    pub fn is_clean(&self) -> bool {
        self.added_dirs.is_empty()
            && self.added_files.is_empty()
            && self.untracked_files.is_empty()
            && self.untracked_dirs.is_empty()
            && self.modified_files.is_empty()
            && self.removed_files.is_empty()
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
        println!("Untracked files:");
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

    pub fn print_removed(&self) {
        println!("Removed files:");
        println!("  (use \"oxen add <file>...\" to update what will be committed)");
        println!("  (use \"oxen restore <file>...\" to discard changes in working directory)");
        self.print_removed_files();
        println!();
    }

    fn print_added_dirs(&self) {
        for (dir, count) in self.added_dirs.iter() {
            // Make sure we can grab the filename
            let added_file_str = format!("  added:  {}/", dir.to_str().unwrap()).green();
            let num_files_str = match count {
                1 => {
                    format!("with added {} file\n", count)
                }
                0 => {
                    // Skip since we don't have any added files in this dir
                    String::from("\n")
                }
                _ => {
                    format!("with added {} files\n", count)
                }
            };
            print!("{} {}", added_file_str, num_files_str);
        }
    }

    fn print_added_files(&self) {
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
        let added_files: Vec<PathBuf> = self.added_files.clone().into_iter().map(|(path, _)| path).collect();
        let mut top_level_counts = self.get_top_level_removed_counts(&repo_path, &added_files);
        // See the actual counts in the dir, if nothing remains, we can just print the top level summary
        let remaining_file_count = self.get_remaining_removed_counts(&mut top_level_counts, &repo_path);
        let mut summarized: HashSet<PathBuf> = HashSet::new();
        for (short_path, entry) in self.added_files.iter() {
            // If the short_path is in a directory that was added, don't display it
            let mut break_both = false;
            for (dir, _size) in self.added_dirs.iter() {
                // println!("checking if short_path {:?} starts with {:?}", short_path, dir);
                if short_path.starts_with(&dir) {
                    break_both = true;
                    continue;
                }
            }

            if break_both {
                continue;
            }

            if entry.status == StagedEntryStatus::Removed {
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
                        short_path.to_str().unwrap(),
                    )
                    .green();
                    println!("{}", added_file_str);

                    summarized.insert(path.to_owned());
                }

                if !summarized.contains(&path) {
                    let added_file_str = format!("  removed:  {}", short_path.to_str().unwrap()).green();
                    println!("{}", added_file_str);
                }
            } else {
                let added_file_str = format!("  added:  {}", short_path.to_str().unwrap()).green();
                println!("{}", added_file_str);
            }
        }
    }

    fn print_modified_files(&self) {
        for file in self.modified_files.iter() {
            let added_file_str = format!("  modified:  {}", file.to_str().unwrap()).yellow();
            println!("{}", added_file_str);
        }
    }

    fn get_top_level_removed_counts(&self, repo_path: &Path, paths: &Vec<PathBuf>) -> HashMap<PathBuf, usize> {
        let mut top_level_counts: HashMap<PathBuf, usize> = HashMap::new();
        for short_path in paths.iter() {
            let full_path = repo_path.join(short_path);

            let path = self.get_top_level_dir(&repo_path, &full_path);
            if !top_level_counts.contains_key(&path) {
                top_level_counts.insert(path.to_path_buf(), 0);
            }
            *top_level_counts.get_mut(&path).unwrap() += 1;
        }
        return top_level_counts;
    }

    fn get_remaining_removed_counts(&self, top_level_counts: &mut HashMap<PathBuf, usize>, repo_path: &Path) -> HashMap<PathBuf, usize> {
        let mut remaining_file_count: HashMap<PathBuf, usize> = HashMap::new();
        for (dir, _) in top_level_counts.iter() {
            let full_path = repo_path.join(dir);

            let count = util::fs::rcount_files_in_dir(&full_path);
            remaining_file_count.insert(dir.to_owned(), count);
        }
        return remaining_file_count;
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
        let mut top_level_counts = self.get_top_level_removed_counts(&repo_path, &self.removed_files);

        // See the actual counts in the dir, if nothing remains, we can just print the top level summary
        let remaining_file_count = self.get_remaining_removed_counts(&mut top_level_counts, &repo_path);

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
        for (dir, count) in self.untracked_dirs.iter() {
            // Make sure we can grab the filename
            if let Some(filename) = dir.file_name() {
                let added_file_str = format!("  {}/", filename.to_str().unwrap()).red();
                let num_files_str = match count {
                    1 => {
                        format!("with untracked {} file\n", count)
                    }
                    0 => {
                        // Skip since we don't have any untracked files in this dir
                        String::from("")
                    }
                    _ => {
                        format!("with untracked {} files\n", count)
                    }
                };

                if !num_files_str.is_empty() {
                    print!("{} {}", added_file_str, num_files_str);
                }
            }
        }
    }

    fn print_untracked_files(&self) {
        // List untracked files
        for file in self.untracked_files.iter() {
            let mut break_both = false;
            // If the file is in a directory that is untracked, don't display it
            for (dir, _size) in self.untracked_dirs.iter() {
                // println!("checking if file {:?} starts with {:?}", file, dir);
                if file.starts_with(&dir) {
                    break_both = true;
                    continue;
                }
            }

            if break_both {
                continue;
            }

            let added_file_str = file.to_str().unwrap().to_string().red();
            println!("  {}", added_file_str);
        }
        println!();
    }
}
