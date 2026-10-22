//! Helper functions to get metadata from audio files.
//!

use crate::{error::OxenError, model::metadata::MetadataAudio};

use lofty::file::{AudioFile, FileType};
use lofty::probe::Probe;
use std::io::{BufReader, Read, Seek};

/// Detects the audio metadata of the file `data` reads, in the format `extension` indicates.
pub fn get_metadata(data: impl Read + Seek, extension: &str) -> Result<MetadataAudio, OxenError> {
    let data = BufReader::new(data);
    let probe = match FileType::from_ext(extension) {
        Some(file_type) => Probe::with_file_type(data, file_type),
        None => Probe::new(data),
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
        Err(err) => Err(OxenError::internal_error(format!(
            "Could not read audio stream as {extension}: {err}"
        ))),
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
            let (_, data_type, staged) =
                repositories::metadata::inspect_file(&version_file, &file)?;
            assert_eq!(data_type, EntryDataType::Audio);
            let Some(GenericMetadata::MetadataAudio(staged)) = staged else {
                panic!("a copy with no extension takes the format its name gives, got {staged:?}");
            };
            assert_eq!(staged.audio.sample_rate, metadata.audio.sample_rate);
            assert_eq!(staged.audio.num_channels, metadata.audio.num_channels);

            let missing = dir.join("missing.wav");
            assert!(
                repositories::metadata::inspect_file(&missing, &missing).is_err(),
                "a file that cannot be opened fails its inspection"
            );
            Ok(())
        })
        .expect("the extensionless copy's metadata matches the named file's");
    }
}
