//! Helper functions to get metadata from audio files.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::MetadataAudio;

use std::path::Path;
use std::sync::Once;

static START: Once = Once::new();

/// Detects the audio metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataAudio, OxenError> {
    START.call_once(|| match ffmpeg::init() {
        Ok(_) => {}
        Err(err) => {
            log::error!("Could not initialize ffmpeg {:?}", err);
        }
    });

    match ffmpeg::format::input(&path) {
        Ok(context) => {
            let duration = context.duration() as f64 / f64::from(ffmpeg::ffi::AV_TIME_BASE);

            // for now just grab the best audio stream
            let stream = context
                .streams()
                .best(ffmpeg::media::Type::Audio)
                .ok_or(OxenError::basic_str("Could not grab audio stream"))?;

            let codec = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
                .map_err(|_| OxenError::basic_str("Could not grab audio codec"))?;

            let audio = codec
                .decoder()
                .audio()
                .map_err(|_| OxenError::basic_str("Could not grab audio decoder"))?;

            Ok(MetadataAudio {
                num_seconds: duration,
                format: "mp3".to_string(), // mp3, etc.
                num_channels: audio.channels() as usize,
                sample_rate: audio.rate() as usize,
            })
        }
        Err(err) => {
            let err = format!("Could not get audio metadata {:?}", err);
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::EntryDataType;
    use crate::test;
    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = api::local::metadata::compute_metadata(file).unwrap();

        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");

        assert!(metadata.meta.audio.is_some());
        let meta = metadata.meta.audio.unwrap();
        assert_eq!(meta.num_channels, 1);
        assert_eq!(meta.sample_rate, 16000);
        assert_relative_eq!(meta.num_seconds, 3.10);
    }
}
