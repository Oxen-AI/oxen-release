pub fn is_glob_path(path: &str) -> bool {
    let glob_chars = ['*', '?', '[', ']'];
    glob_chars.iter().any(|c| path.contains(*c))
}
