//! Gzip decompression helpers.

use std::io::{Read, Write};

use flate2::read::GzDecoder;

use crate::error::OxenError;

/// Inflate gzipped `data`, erroring if the inflated output would exceed `max_decompressed` bytes.
/// `max_decompressed` protects against gzip bomb attacks by limiting the decompressed size.
pub fn decompress_gzip_capped(data: &[u8], max_decompressed: u64) -> Result<Vec<u8>, OxenError> {
    let mut decompressed = Vec::with_capacity(data.len());
    decompress_gzip_capped_into(data, &mut decompressed, max_decompressed)?;
    Ok(decompressed)
}

/// Inflate the gzipped bytes `reader` yields into `writer`, erroring once the inflated output
/// passes `max_decompressed` bytes, and return how many bytes it wrote. On an error, `writer` may
/// hold part of the output.
pub fn decompress_gzip_capped_into(
    reader: impl Read,
    writer: &mut impl Write,
    max_decompressed: u64,
) -> Result<u64, OxenError> {
    let mut decoder = GzDecoder::new(reader).take(max_decompressed + 1);
    let decompressed_size = std::io::copy(&mut decoder, writer).map_err(|e| {
        OxenError::internal_error(format!("Failed to decompress gzipped data: {e}"))
    })?;
    if decompressed_size > max_decompressed {
        return Err(OxenError::internal_error(format!(
            "Decompressed size {decompressed_size} exceeds the {max_decompressed} byte limit"
        )));
    }
    Ok(decompressed_size)
}
