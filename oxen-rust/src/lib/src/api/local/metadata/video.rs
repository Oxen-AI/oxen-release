//! Helper functions to get metadata from the video files.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::{ImgColorSpace, MetadataVideo};

use std::path::Path;
use std::sync::Once;

static START: Once = Once::new();

/// Detects the video metadata for the given file.
pub fn get_metadata(path: impl AsRef<Path>) -> Result<MetadataVideo, OxenError> {
    START.call_once(|| match ffmpeg::init() {
        Ok(_) => {}
        Err(err) => {
            log::error!("Could not initialize ffmpeg {:?}", err);
        }
    });

    match ffmpeg::format::input(&path) {
        Ok(context) => {
            let duration = context.duration() as f64 / f64::from(ffmpeg::ffi::AV_TIME_BASE);

            // for now just grab the best video stream
            let stream = context
                .streams()
                .best(ffmpeg::media::Type::Video)
                .ok_or(OxenError::basic_str("Could not grab video stream"))?;

            let codec = ffmpeg::codec::context::Context::from_parameters(stream.parameters())
                .map_err(|_| OxenError::basic_str("Could not grab video codec"))?;

            let video = codec
                .decoder()
                .video()
                .map_err(|_| OxenError::basic_str("Could not grab video decoder"))?;

            Ok(MetadataVideo {
                width: video.width() as usize,
                height: video.height() as usize,
                color_space: ffmpg_to_colorspace(&video.format()), // RGB, RGBA, etc.
                num_seconds: duration,
                format: context.format().name().to_string(), // mp4, etc.
            })
        }
        Err(err) => {
            let err = format!("Could not get video metadata {:?}", err);
            Err(OxenError::basic_str(err))
        }
    }
}

fn ffmpg_to_colorspace(format: &ffmpeg::util::format::pixel::Pixel) -> ImgColorSpace {
    println!("ffmpeg colorspace: {:?}", format);
    match format {
        ffmpeg::util::format::pixel::Pixel::RGBA => ImgColorSpace::RGBA,
        ffmpeg::util::format::pixel::Pixel::GRAY8 => ImgColorSpace::Grayscale,
        _ => ImgColorSpace::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use crate::api;
    use crate::model::EntryDataType;
    use crate::test;

    use approx::assert_relative_eq;

    #[test]
    fn test_get_metadata_video_mp4() {
        let file = test::test_video_file_with_name("basketball.mp4");
        let metadata = api::local::metadata::compute_metadata(file).unwrap();
        println!("metadata: {:?}", metadata);

        assert_eq!(metadata.size, 23599);
        assert_eq!(metadata.data_type, EntryDataType::Video);
        assert_eq!(metadata.mime_type, "video/mp4");

        assert!(metadata.meta.video.is_some());
        let meta = metadata.meta.video.unwrap();
        assert_eq!(meta.width, 128);
        assert_eq!(meta.height, 176);
        assert_relative_eq!(meta.num_seconds, 1.6);
    }
}
