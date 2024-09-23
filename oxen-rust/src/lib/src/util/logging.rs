use env_logger::Env;
use std::io::Write;

#[macro_export]
macro_rules! current_function {
    () => {{
        fn f() {}
        fn type_name_of<T>(_: T) -> &'static str {
            std::any::type_name::<T>()
        }
        let name = type_name_of(f);
        &name[..name.len() - 3]
    }};
}

pub fn init_logging() {
    env_logger::Builder::from_env(Env::default())
        .format(|buf, record| {
            fn truncate_from_left(s: &str, max_chars: usize) -> String {
                s.chars()
                    .skip(s.chars().count().saturating_sub(max_chars))
                    .collect()
            }

            writeln!(
                buf,
                "{} {} {}:{} [{}] - {}: {}",
                chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.3f"),
                record.module_path().unwrap_or(""),
                truncate_from_left(record.file().unwrap_or("unknown"), 40),
                record.line().unwrap_or(0),
                record.level(),
                record.target(),
                record.args()
            )
        })
        .init();
}
