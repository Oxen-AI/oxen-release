use polars::io::mmap::MmapBytesReader;

use crate::core::index::file_chunker::CHUNK_SIZE;
use crate::core::index::merkle_tree::node::FileNode;
use crate::model::LocalRepository;
use crate::util;

use std::io::Read;
use std::io::Seek;

pub struct ChunkReader {
    repo: LocalRepository,
    node: FileNode,
    offset: u64,
}

impl ChunkReader {
    pub fn new(repo: LocalRepository, node: FileNode) -> Self {
        Self {
            repo,
            node,
            offset: 0,
        }
    }
}

impl Read for ChunkReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        log::debug!(
            "--start-- read from chunked file at offset {:?}",
            self.offset
        );
        log::debug!("Total bytes {:?}", self.node.num_bytes);
        log::debug!("Buffer length {:?}", buf.len());
        // FileNode has a vector of chunks
        // Each chunk has a size of CHUNK_SIZE
        // We need to read the chunk at the offset and copy the data to the buffer

        // chunk_index is which file chunk we are reading
        let mut chunk_index = self.offset / CHUNK_SIZE as u64;
        // chunk_offset is the offset within the chunk
        let mut chunk_offset = self.offset % CHUNK_SIZE as u64;

        log::debug!("Chunk index: {:?} offset {:?}", chunk_index, chunk_offset);

        if chunk_index >= self.node.chunk_hashes.len() as u64 {
            return Ok(0);
        }

        // read chunks until we fill the buffer
        let mut total_read = 0;
        while total_read < buf.len() as u64 {
            log::debug!(
                "chunk_index {:?} chunk_offset {:?}",
                chunk_index,
                chunk_offset
            );
            // Find the hashed chunk file
            let chunk_hash = self.node.chunk_hashes[chunk_index as usize];
            let hash_str = format!("{:x}", chunk_hash);
            let chunk_path = util::fs::chunk_path(&self.repo, &hash_str);
            log::debug!(
                "Opening chunk [{} / {}] file at {:?}",
                chunk_index,
                self.node.chunk_hashes.len(),
                chunk_path
            );
            let mut file = std::fs::File::open(chunk_path).unwrap();
            let mut file_data = Vec::new();
            file.read_to_end(&mut file_data).unwrap();
            let file_data_len = file_data.len() as u64;

            log::debug!("Chunk file size {:?}", file_data_len);

            let bytes_to_copy =
                std::cmp::min(buf.len() as u64 - total_read, file_data_len - chunk_offset);
            log::debug!("Bytes to copy {:?}", bytes_to_copy);
            buf[total_read as usize..(total_read + bytes_to_copy) as usize].copy_from_slice(
                &file_data[chunk_offset as usize..(chunk_offset + bytes_to_copy) as usize],
            );
            total_read += bytes_to_copy;
            chunk_offset += bytes_to_copy;

            if chunk_offset >= CHUNK_SIZE as u64 {
                chunk_offset = 0;
            }

            chunk_index += 1;
            self.offset += bytes_to_copy;
            log::debug!("Total read {:?}/{}", total_read, buf.len());
            log::debug!("Offset {:?}", self.offset);
        }

        log::debug!("--end-- Total read {:?}", total_read);

        Ok(total_read as usize)
    }
}

impl Seek for ChunkReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        log::debug!("Seek in chunked file {:?}", pos);
        self.offset = match pos {
            std::io::SeekFrom::Start(offset) => offset,
            std::io::SeekFrom::Current(offset) => self.offset + offset as u64,
            std::io::SeekFrom::End(offset) => (self.node.num_bytes as i64 + offset) as u64,
        };
        log::debug!("New offset {:?}", self.offset);
        Ok(self.offset)
    }
}

impl MmapBytesReader for ChunkReader {
    fn to_file(&self) -> Option<&std::fs::File> {
        todo!("File for chunked file")
    }

    fn to_bytes(&self) -> Option<&[u8]> {
        let mut bytes = vec![0; self.node.num_bytes as usize];
        self.read(&mut bytes).unwrap();
        Some(&bytes)
    }
}
