pub fn split_and_trim(input: &str, delimiter: &str) -> Vec<String> {
    input
        .split(delimiter)
        .map(|v| v.trim())
        .map(String::from)
        .collect::<Vec<String>>()
}
