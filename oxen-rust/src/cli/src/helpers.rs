pub fn is_glob_path(path: &str) -> bool {
    let glob_chars = vec!['*', '?', '[', ']'];
    glob_chars.iter().any(|c| path.contains(*c))
}
