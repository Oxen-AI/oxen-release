use colored::Colorize;
use std::path::PathBuf;

pub struct StagedData {
    pub added_dirs: Vec<(PathBuf, usize)>,
    pub added_files: Vec<PathBuf>,
    // TODO: this hack might not work anymore...because we will have to track if any file changed in the directory
    pub untracked_dirs: Vec<(PathBuf, usize)>,
    pub untracked_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
}

impl StagedData {
    pub fn is_clean(&self) -> bool {
        self.added_dirs.is_empty()
            && self.added_files.is_empty()
            && self.untracked_files.is_empty()
            && self.untracked_dirs.is_empty()
            && self.modified_files.is_empty()
    }

    pub fn has_added_entries(&self) -> bool {
        !self.added_dirs.is_empty() || !self.added_files.is_empty()
    }

    pub fn has_modified_entries(&self) -> bool {
        !self.modified_files.is_empty()
    }

    pub fn has_untracked_entries(&self) -> bool {
        !self.untracked_dirs.is_empty() || !self.untracked_files.is_empty()
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

    fn print_added_dirs(&self) {
        for (dir, count) in self.added_dirs.iter() {
            // Make sure we can grab the filename
            let added_file_str = format!("  added:  {}/", dir.to_str().unwrap()).green();
            let num_files_str = match count {
                1 => {
                    format!("with untracked {} file\n", count)
                }
                0 => {
                    // Skip since we don't have any untracked files in this dir
                    String::from("\n")
                }
                _ => {
                    format!("with untracked {} files\n", count)
                }
            };
            print!("{} {}", added_file_str, num_files_str);
        }
    }

    fn print_added_files(&self) {
        for file in self.added_files.iter() {
            // If the file is in a directory that was added, don't display it
            let mut break_both = false;
            for (dir, _size) in self.added_dirs.iter() {
                // println!("checking if file {:?} starts with {:?}", file, dir);
                if file.starts_with(&dir) {
                    break_both = true;
                    continue;
                }
            }

            if break_both {
                continue;
            }

            let added_file_str = format!("  added:  {}", file.to_str().unwrap()).green();
            println!("{}", added_file_str);
        }
    }

    fn print_modified_files(&self) {
        for file in self.modified_files.iter() {
            let added_file_str = format!("  modified:  {}", file.to_str().unwrap()).yellow();
            println!("{}", added_file_str);
        }
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
