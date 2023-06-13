//! Helper functions to get metadata from the images.
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::MetaDataImage;

// use magick_rust::{magick_wand_genesis, MagickWand};

use std::path::Path;
// use std::sync::Once;

// static START: Once = Once::new();

/// Detects the image metadata for the given file.
pub fn get_metadata(_path: impl AsRef<Path>) -> Result<MetaDataImage, OxenError> {
    // START.call_once(|| {
    //     magick_wand_genesis();
    // });

    // let path = path.as_ref();

    // let wand = MagickWand::new();
    // match wand.ping_image(&path.to_string_lossy()) {
    //     Ok(_) => {}
    //     Err(err) => {
    //         let err = format!("Could not get image metadata {:?} Err {:?}", path, err);
    //         log::warn!("{}", err);
    //         return Err(OxenError::image_metadata_error(&err));
    //     }
    // }

    // let format = match wand.get_image_format() {
    //     Ok(format) => format,
    //     Err(err) => {
    //         log::warn!("Could not get image format for {:?} Err {:?}", path, err);
    //         "Unknown".to_string()
    //     }
    // };

    // Ok(MetaDataImage {
    //     width: wand.get_image_width(),
    //     height: wand.get_image_height(),
    //     color_space: magick_to_colorspace(&wand),
    //     format,
    // })
    Err(OxenError::basic_str("Could not get image metadata"))
}

// fn magick_to_colorspace(wand: &MagickWand) -> ImgColorSpace {
//     let colorspace = wand.get_image_colorspace();
//     match colorspace {
//         magick_rust::bindings::ColorspaceType_RGBColorspace
//         | magick_rust::bindings::ColorspaceType_sRGBColorspace => {
//             if wand.get_image_alpha_channel() {
//                 ImgColorSpace::RGBA
//             } else {
//                 ImgColorSpace::RGB
//             }
//         }
//         magick_rust::bindings::ColorspaceType_GRAYColorspace => ImgColorSpace::Grayscale,
//         _ => ImgColorSpace::Unknown,
//     }
// }

#[cfg(test)]
mod tests {
    
    
    
    

    #[test]
    fn test_get_metadata_img_rgb() {
        // let file = test::test_img_file_with_name("cat_1.jpg");

        // let data = api::local::metadata::compute_metadata(file).unwrap();

        // assert_eq!(data.data_type, EntryDataType::Image);
        // assert_eq!(data.mime_type, "image/jpeg");

        // let metadata = data.meta.image.unwrap();
        // assert_eq!(metadata.width, 499);
        // assert_eq!(metadata.height, 375);
        // assert_eq!(metadata.color_space, ImgColorSpace::RGB);
        // assert_eq!(metadata.format, "JPEG");
    }

    #[test]
    fn test_get_metadata_img_rgba() {
        // let file = test::test_img_file_with_name("cat_rgba.png");
        // let data = api::local::metadata::compute_metadata(file).unwrap();

        // assert_eq!(data.data_type, EntryDataType::Image);
        // assert_eq!(data.mime_type, "image/png");

        // let metadata = data.meta.image.unwrap();

        // assert_eq!(metadata.width, 499);
        // assert_eq!(metadata.height, 375);
        // assert_eq!(metadata.color_space, ImgColorSpace::RGBA);
        // assert_eq!(metadata.format, "PNG");
    }

    #[test]
    fn test_get_metadata_img_png_no_ext() {
        // let file = test::test_img_file_with_name("cat_no_ext");
        // let data = api::local::metadata::compute_metadata(file).unwrap();

        // assert_eq!(data.data_type, EntryDataType::Image);
        // assert_eq!(data.mime_type, "image/png");
        // let metadata = data.meta.image.unwrap();

        // assert_eq!(metadata.width, 499);
        // assert_eq!(metadata.height, 375);
        // assert_eq!(metadata.color_space, ImgColorSpace::RGBA);
        // assert_eq!(metadata.format, "PNG");
    }

    #[test]
    fn test_get_metadata_img_grayscale() {
        // let file = test::test_img_file_with_name("cat_grayscale.jpg");

        // let data = api::local::metadata::compute_metadata(file).unwrap();

        // assert_eq!(data.data_type, EntryDataType::Image);
        // assert_eq!(data.mime_type, "image/jpeg");

        // let metadata = data.meta.image.unwrap();

        // assert_eq!(metadata.width, 499);
        // assert_eq!(metadata.height, 375);
        // assert_eq!(metadata.color_space, ImgColorSpace::Grayscale);
        // assert_eq!(metadata.format, "JPEG");
    }

    #[test]
    fn test_get_metadata_img_mnist() {
        // let file = test::test_img_file_with_name("mnist_7.png");
        // let data = api::local::metadata::compute_metadata(file).unwrap();

        // assert_eq!(data.data_type, EntryDataType::Image);
        // assert_eq!(data.mime_type, "image/png");

        // let metadata = data.meta.image.unwrap();
        // assert_eq!(metadata.width, 28);
        // assert_eq!(metadata.height, 28);
        // assert_eq!(metadata.color_space, ImgColorSpace::Grayscale);
        // assert_eq!(metadata.format, "PNG");
    }
}
