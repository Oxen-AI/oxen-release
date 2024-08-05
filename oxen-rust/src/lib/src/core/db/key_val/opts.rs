use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, DBCompactionStyle, DBCompressionType, LogLevel, Options,
};

pub fn default() -> Options {
    let mut opts = Options::default();
    opts.set_log_level(LogLevel::Fatal);
    opts.create_if_missing(true);
    opts.set_max_log_file_size(0);
    opts.set_keep_log_file_num(1);
    opts.set_max_manifest_file_size(1);
    opts.set_max_file_opening_threads(num_cpus::get() as i32);
    opts.set_skip_stats_update_on_db_open(true);
    let max_open_files = std::env::var("MAX_OPEN_FILES")
        .map_or(128, |v| v.parse().expect("MAX_OPEN_FILES must be a number"));
    opts.set_max_open_files(max_open_files);

    // From Claude Anthropic
    // opts.set_bloom_locality(10);
    // opts.set_max_open_files(-1); // Use as many as the OS allows
    // opts.set_compaction_style(DBCompactionStyle::Level);
    // opts.increase_parallelism(num_cpus::get() as i32);
    // opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(5));
    // opts.set_level_compaction_dynamic_level_bytes(true);
    // opts.set_disable_auto_compactions(true);
    // opts.set_compression_type(DBCompressionType::Lz4);
    // let mut block_opts = BlockBasedOptions::default();
    // block_opts.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    // opts.set_block_based_table_factory(&block_opts);

    opts.set_compression_type(DBCompressionType::Zstd);
    opts.set_compaction_style(DBCompactionStyle::Level);
    opts.set_target_file_size_base(16 * 1024 * 1024); // 16MB
    opts.set_write_buffer_size(16 * 1024 * 1024); // 16MB
    opts.set_max_bytes_for_level_base(16 * 1024 * 1024); // 16MB
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    opts.set_block_based_table_factory(&block_opts);

    opts
}
