//! Helper functions to get metadata from audio files.
//!

use crate::{error::OxenError, model::metadata::MetadataAudio};

use lofty::file::{AudioFile, FileType};
use lofty::probe::Probe;
use std::path::Path;

/// Detects the audio metadata for the file at `path`, read as the format `extension` indicates.
pub fn get_metadata(path: &Path, extension: &str) -> Result<MetadataAudio, OxenError> {
    match Probe::open(path) {
        Ok(probe) => {
            let probe = match FileType::from_ext(extension) {
                Some(file_type) => probe.set_file_type(file_type),
                None => probe,
            };
            match probe.read() {
                Ok(tagged_file) => {
                    let properties = tagged_file.properties();
                    let duration = properties.duration();
                    let seconds = duration.as_secs_f64();
                    let rate = properties.sample_rate().unwrap_or(0);
                    let channels = properties.channels().unwrap_or(0);

                    Ok(MetadataAudio::new(
                        seconds,
                        channels as usize,
                        rate as usize,
                    ))
                }
                Err(err) => {
                    let error_str = format!("Could not read audio stream from {path:?} {err}");
                    Err(OxenError::basic_str(error_str))
                }
            }
        }
        Err(err) => {
            let error_str = format!("Could not probe audio stream from {path:?}: {err}");
            Err(OxenError::basic_str(error_str))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::model::metadata::MetadataAudio;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::repositories;
    use crate::test;
    use crate::util;

    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_audio_flac() {
        let file = test::test_audio_file_with_name("121-121726-0005.flac");
        let metadata = repositories::metadata::get(file).unwrap();

        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, 37096);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-flac");

        assert!(metadata.metadata.is_some());
        let metadata: MetadataAudio = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataAudio(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.audio.num_channels, 1);
        assert_eq!(metadata.audio.sample_rate, 16000);
        assert_relative_eq!(metadata.audio.num_seconds, 3.1);
    }

    #[test]
    fn test_get_metadata_audio_wav() {
        let file = test::test_audio_file_with_name("121-121726-0005.wav");
        let metadata = repositories::metadata::get(&file).unwrap();

        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, 99278);
        assert_eq!(metadata.data_type, EntryDataType::Audio);
        assert_eq!(metadata.mime_type, "audio/x-wav");

        assert!(metadata.metadata.is_some());
        let metadata: MetadataAudio = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataAudio(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.audio.num_channels, 1);
        assert_eq!(metadata.audio.sample_rate, 16000);
        assert_relative_eq!(metadata.audio.num_seconds, 3.1);

        test::run_empty_dir_test(|dir| {
            let version_file = dir.join("data");
            util::fs::copy(&file, &version_file)?;
            let staged = repositories::metadata::get_file_metadata_with_extension(
                &version_file,
                &EntryDataType::Audio,
                "wav",
            )?;
            let Some(GenericMetadata::MetadataAudio(staged)) = staged else {
                panic!("a copy with no extension is probed in the format given, got {staged:?}");
            };
            assert_eq!(staged.audio.sample_rate, metadata.audio.sample_rate);
            assert_eq!(staged.audio.num_channels, metadata.audio.num_channels);
            Ok(())
        })
        .expect("the extensionless copy's metadata matches the named file's");
    }
}
