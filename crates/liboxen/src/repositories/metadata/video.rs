//! Helper functions to get metadata from the video files.
//!

use crate::{error::OxenError, model::metadata::MetadataVideo};

use mp4::{Mp4Reader, Mp4Track, TrackType};
use std::io::{BufReader, Read, Seek, SeekFrom};

/// Detects the video metadata of the file `data` reads.
pub fn get_metadata(mut data: impl Read + Seek) -> Result<MetadataVideo, OxenError> {
    let size = data.seek(SeekFrom::End(0))?;
    data.rewind()?;

    match Mp4Reader::read_header(BufReader::new(data), size) {
        Ok(video) => {
            let duration = video.duration().as_secs_f64();

            let video_tracks: Vec<&Mp4Track> = video
                .tracks()
                .values()
                .filter_map(|t| match t.track_type() {
                    Ok(TrackType::Video) => Some(Ok(t)),
                    Ok(_) => None,
                    Err(e) => Some(Err(OxenError::basic_str(format!(
                        "Could not get track type: {e:?}"
                    )))),
                })
                .collect::<Result<Vec<_>, _>>()?;

            let video = video_tracks
                .first()
                .ok_or_else(|| OxenError::basic_str("Could not get video track"))?;

            Ok(MetadataVideo::new(
                duration,
                video.width() as usize,
                video.height() as usize,
            ))
        }
        Err(err) => {
            let err = format!("Could not get video metadata {err:?}");
            Err(OxenError::basic_str(err))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::model::EntryDataType;
    use crate::model::metadata::MetadataVideo;
    use crate::model::metadata::generic_metadata::GenericMetadata;
    use crate::repositories;
    use crate::test;

    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_video_mp4() {
        let file = test::test_video_file_with_name("basketball.mp4");
        let metadata = repositories::metadata::get(file).unwrap();
        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, 23599);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/mp4");

        let metadata: MetadataVideo = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataVideo(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.video.width, 128);
        assert_eq!(metadata.video.height, 176);
        assert_relative_eq!(metadata.video.num_seconds, 1.6);
    }

    #[test]
    fn test_get_metadata_video_mov() {
        let file = test::test_video_file_with_name("dog_skatez.mov");
        let expected_size = std::fs::metadata(&file).unwrap().len();
        let metadata = repositories::metadata::get(&file).unwrap();
        println!("metadata: {metadata:?}");

        assert_eq!(metadata.size, expected_size);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/quicktime");

        let metadata: MetadataVideo = match metadata.metadata.unwrap() {
            GenericMetadata::MetadataVideo(metadata) => metadata,
            _ => panic!("Wrong metadata type"),
        };

        assert_eq!(metadata.video.width, 64);
        assert_eq!(metadata.video.height, 64);
        assert_relative_eq!(metadata.video.num_seconds, 1.0);
    }
}
