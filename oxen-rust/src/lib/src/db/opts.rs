
use rocksdb::{LogLevel, Options};

pub fn default() -> Options {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    opts.create_if_missing(true);
    opts.set_max_log_file_size(0);
    opts.set_keep_log_file_num(1);
    opts
}
