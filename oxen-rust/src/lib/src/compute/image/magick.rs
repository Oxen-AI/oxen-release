//! Helper functions to get compute stats on images
//!

use crate::error::OxenError;
use crate::model::entry::metadata_entry::{ImgColorSpace, MetaDataImage};

use magick_rust::{magick_wand_genesis, MagickWand};

use std::path::Path;
use std::sync::Once;

static START: Once = Once::new();

/// Detects the image metadata for the given file.
pub fn get_image_metadata(path: impl AsRef<Path>) -> Result<MetaDataImage, OxenError> {
    START.call_once(|| {
        magick_wand_genesis();
    });

    let path = path.as_ref();

    let wand = MagickWand::new();
    match wand.ping_image(&path.to_string_lossy()) {
        Ok(_) => {}
        Err(err) => {
            let err = format!("Could not get image metadata {:?} Err {:?}", path, err);
            log::warn!("{}", err);
            return Err(OxenError::image_metadata_error(&err));
        }
    }

    let format = match wand.get_image_format() {
        Ok(format) => format,
        Err(err) => {
            log::warn!("Could not get image format for {:?} Err {:?}", path, err);
            "Unknown".to_string()
        }
    };

    Ok(MetaDataImage {
        width: wand.get_image_width(),
        height: wand.get_image_height(),
        color_space: magick_to_oxen_colorspace(&wand),
        format,
    })
}

fn magick_to_oxen_colorspace(wand: &MagickWand) -> ImgColorSpace {
    let colorspace = wand.get_image_colorspace();
    match colorspace {
        magick_rust::bindings::ColorspaceType_RGBColorspace
        | magick_rust::bindings::ColorspaceType_sRGBColorspace => {
            if wand.get_image_alpha_channel() {
                ImgColorSpace::RGBA
            } else {
                ImgColorSpace::RGB
            }
        }
        magick_rust::bindings::ColorspaceType_GRAYColorspace => ImgColorSpace::Grayscale,
        _ => ImgColorSpace::Unknown,
    }
}
