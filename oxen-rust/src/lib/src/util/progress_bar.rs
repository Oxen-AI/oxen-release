use std::sync::Arc;

use indicatif::{ProgressBar, ProgressStyle};

pub enum ProgressBarType {
    Counter,
    Bytes,
    None,
}

pub fn oxen_progress_bar(size: u64, progress_type: ProgressBarType) -> Arc<ProgressBar> {
    let bar = Arc::new(ProgressBar::new(size));
    bar.set_style(
        ProgressStyle::default_bar()
            .template(progress_type_to_template(progress_type).as_str())
            .unwrap()
            .progress_chars("🌾🐂➖"),
    );
    bar
}

fn progress_type_to_template(progress_type: ProgressBarType) -> String {
    match progress_type {
        ProgressBarType::Counter => {
            "{spinner:.green} [{elapsed_precise}] [{bar:60}] {pos}/{len} ({eta})".to_string()
        }
        ProgressBarType::Bytes => {
            "{spinner:.green} [{elapsed_precise}] [{bar:60}] {bytes}/{total_bytes} ({eta})"
                .to_string()
        }
        ProgressBarType::None => "{spinner:.green} [{elapsed_precise}] [{bar:60}]".to_string(),
    }
}
