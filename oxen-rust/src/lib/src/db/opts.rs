use rocksdb::{LogLevel, Options};

pub fn default() -> Options {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    opts.create_if_missing(true);
    opts.set_max_log_file_size(0);
    opts.set_keep_log_file_num(1);
    let max_open_files = std::env::var("MAX_OPEN_FILES")
        .map_or(128, |v| v.parse().expect("MAX_OPEN_FILES must be a number"));
    opts.set_max_open_files(max_open_files);
    opts
}
