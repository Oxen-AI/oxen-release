
use indicatif::ProgressBar;
use std::sync::Arc;

pub struct ReadProgress<R> {
    pub inner: R,
    pub progress_bar: Arc<ProgressBar>,
}

impl<R: std::io::Read> std::io::Read for ReadProgress<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size = self.inner.read(buf).map(|n| {
            self.progress_bar.inc(n as u64);
            n
        });
        if self.progress_bar.elapsed() >= self.progress_bar.duration() {
            self.progress_bar.finish();
        }
        size
    }
}