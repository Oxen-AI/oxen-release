/*
Write a db that is optimized for opening, finding by hash, listing.

Rocks db is too slow. It was taking ~100ms to open a db, and if we have > 10 vnodes,
that means we are taking > 1 second to open before doing any operations.

We can make this faster by using a simple file format.

Writing happens once at commit, then we read many times from the server and status.

Is also already sharded and optimized in the tree structure.
Reading, find by hash, listing is high throughput.

On Disk Format:

All nodes are stored in .oxen/tree/{NODE_HASH} and contain two files:
- index: the lookup table for all the children
- data: the serialized nodes

index file format:
- dtype
- num_children
- data-type,hash-int,data-offset,data-length

data file format:
- data blobs


For example, data for a vnode of hash 1234 with two children:

.oxen/tree/1234/index
    0 # dtype
    4 # name length
    oxen # name
    2 # num_children

    0 # file data type
    1235 # hash
    0 # data offset
    100 # data length

    1 # dir data type
    1236 # hash
    100 # data offset
    100 # data length

.oxen/tree/1234/data
    {file data node}
    {dir data node}
*/

use rmp_serde::Serializer;
use serde::{de, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::fs::File;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::core::v2::index::merkle_tree::node::CommitMerkleTreeNode;
use crate::core::v2::index::merkle_tree::node::MerkleTreeNodeType;
use crate::error::OxenError;
use crate::util;

pub struct MerkleNodeLookup {
    pub dtype: MerkleTreeNodeType,
    pub name: String,
    pub num_children: u64,
    // hash -> (dtype, offset, length)
    pub offsets: HashMap<u128, (u8, u64, u64)>,
}

impl MerkleNodeLookup {
    pub fn load(lookup_table_file: &mut File) -> Result<Self, OxenError> {
        // Read the whole index into memory
        let mut file_data = Vec::new();
        lookup_table_file.read_to_end(&mut file_data)?;

        // Create a cursor to iterate over data
        let mut cursor = std::io::Cursor::new(file_data);

        // Read the dtype of the node
        let mut buffer = [0u8; 1]; // u8 is 1 byte
        cursor.read_exact(&mut buffer)?;
        let dtype = MerkleTreeNodeType::from_u8(buffer[0]);
        log::debug!("MerkleNodeLookup.load() dtype: {:?}", dtype);

        // Read the length of the name
        let mut buffer = [0u8; 8]; // u64 is 8 bytes
        cursor.read_exact(&mut buffer)?;
        let name_len = u64::from_le_bytes(buffer);
        log::debug!("MerkleNodeLookup.load() name_len: {}", name_len);

        // Read the name
        let mut buffer = vec![0u8; name_len as usize];
        cursor.read_exact(&mut buffer)?;
        let name = String::from_utf8(buffer)?;
        log::debug!("MerkleNodeLookup.load() name: {}", name);

        // Read the number of children in the node
        let mut buffer = [0u8; 8]; // u64 is 8 bytes
        cursor.read_exact(&mut buffer)?;
        let num_children = u64::from_le_bytes(buffer);
        log::debug!("MerkleNodeLookup.load() num_children: {}", num_children);

        // Read the map of offsets
        let mut offsets: HashMap<u128, (u8, u64, u64)> = HashMap::new();
        offsets.reserve(num_children as usize);

        for i in 0..num_children {
            log::debug!("MerkleNodeLookup.load() --reading-- {}", i);
            let mut buffer = [0u8; 1]; // data-type u8 is 1 byte
            cursor.read_exact(&mut buffer)?;
            let data_type = u8::from_le_bytes(buffer);
            log::debug!(
                "MerkleNodeLookup.load() got data_type {:?}",
                MerkleTreeNodeType::from_u8(data_type)
            );

            let mut buffer = [0u8; 16]; // hash u128 is 16 bytes
            cursor.read_exact(&mut buffer)?;
            let hash = u128::from_le_bytes(buffer);
            log::debug!("MerkleNodeLookup.load() got hash {}", hash);

            let mut buffer = [0u8; 8]; // data-offset u64 is 8 bytes
            cursor.read_exact(&mut buffer)?;
            let data_offset = u64::from_le_bytes(buffer);
            log::debug!("MerkleNodeLookup.load() got data_offset {}", data_offset);

            let mut buffer = [0u8; 8]; // data-length u64 is 8 bytes
            cursor.read_exact(&mut buffer)?;
            let data_len = u64::from_le_bytes(buffer);
            log::debug!("MerkleNodeLookup.load() got data_len {}", data_len);

            offsets.insert(hash, (data_type, data_offset, data_len));
        }

        Ok(Self {
            name,
            num_children,
            dtype,
            offsets,
        })
    }
}

pub struct MerkleNodeDB {
    read_only: bool,
    path: PathBuf,
    data_file: Option<File>,
    lookup_file: Option<File>,
    lookup: Option<MerkleNodeLookup>,
    name: String,
    num_children: u64,
    dtype: MerkleTreeNodeType,
    data_offset: u64,
}

impl MerkleNodeDB {
    pub fn num_children(&self) -> u64 {
        if let Some(lookup) = &self.lookup {
            return lookup.num_children;
        }

        self.num_children
    }

    pub fn dtype(&self) -> MerkleTreeNodeType {
        if let Some(lookup) = &self.lookup {
            return lookup.dtype.to_owned();
        }

        self.dtype.to_owned()
    }

    pub fn name(&self) -> String {
        if let Some(lookup) = &self.lookup {
            return lookup.name.to_owned();
        }

        self.name.to_owned()
    }

    pub fn path(&self) -> PathBuf {
        self.path.to_owned()
    }

    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        Self::open(path, true)
    }

    pub fn open_read_write(path: impl AsRef<Path>) -> Result<Self, OxenError> {
        Self::open(path, false)
    }

    fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Self, OxenError> {
        let path = path.as_ref();

        // mkdir if not exists
        if !path.exists() {
            util::fs::create_dir_all(path)?;
        }

        let index_path = path.join("index");
        let data_path = path.join("data");

        log::debug!("Opening merkle node db at {}", path.display());
        let (lookup, lookup_file, data_file): (
            Option<MerkleNodeLookup>,
            Option<File>,
            Option<File>,
        ) = if read_only {
            let mut lookup_file = File::open(index_path)?;
            let data_file = File::open(data_path)?;
            (
                Some(MerkleNodeLookup::load(&mut lookup_file)?),
                Some(lookup_file),
                Some(data_file),
            )
        } else {
            // self.lookup does not exist yet if we are writing (only write once)
            let lookup_file = File::create(index_path)?;
            let data_file = File::create(data_path)?;
            (None, Some(lookup_file), Some(data_file))
        };

        Ok(Self {
            read_only,
            path: path.to_path_buf(),
            data_file,
            lookup_file,
            lookup,
            name: "".to_string(),
            num_children: 0,
            dtype: MerkleTreeNodeType::VNode,
            data_offset: 0,
        })
    }

    pub fn close(&mut self) -> Result<(), OxenError> {
        if let Some(data_file) = &mut self.data_file {
            data_file.flush()?;
            data_file.sync_data()?;
        } else {
            return Err(OxenError::basic_str("Must call open before closing"));
        }

        if let Some(lookup_file) = &mut self.lookup_file {
            lookup_file.flush()?;
            lookup_file.sync_data()?;
        } else {
            return Err(OxenError::basic_str("Must call open before closing"));
        }

        self.data_file = None;
        self.lookup_file = None;
        self.lookup = None;
        Ok(())
    }

    pub fn write_meta(
        &mut self,
        name: &str,
        dtype: MerkleTreeNodeType,
        num_children: u64,
    ) -> Result<(), OxenError> {
        if self.read_only {
            return Err(OxenError::basic_str("Cannot write to read-only db"));
        }

        if self.num_children > 0 {
            return Err(OxenError::basic_str("Cannot write size twice"));
        }

        if self.data_offset > 0 {
            return Err(OxenError::basic_str("Cannot write size after writing data"));
        }

        let Some(lookup_file) = self.lookup_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open before writing"));
        };

        // Write node type
        lookup_file.write_all(&dtype.to_u8().to_le_bytes())?;

        // Write name length
        lookup_file.write_all(&name.len().to_le_bytes())?;

        // Write name
        lookup_file.write_all(name.as_bytes())?;

        // Write num_children
        lookup_file.write_all(&num_children.to_le_bytes())?;

        self.num_children = num_children;
        self.dtype = dtype;
        Ok(())
    }

    pub fn write_one<S: Serialize + Debug>(
        &mut self,
        hash: u128,
        dtype: MerkleTreeNodeType,
        item: &S,
    ) -> Result<(), OxenError> {
        if self.read_only {
            return Err(OxenError::basic_str("Cannot write to read-only db"));
        }

        if self.num_children == 0 {
            return Err(OxenError::basic_str(
                "Must call write_meta() before writing",
            ));
        }

        let Some(lookup_file) = self.lookup_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open() before writing"));
        };
        let Some(data_file) = self.data_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open() before writing"));
        };

        // TODO: Abstract and re-use in write_all
        let mut buf = Vec::new();
        item.serialize(&mut Serializer::new(&mut buf)).unwrap();
        let data_len = buf.len() as u64;
        // log::debug!("--write_one-- dtype {:?}", dtype);
        // log::debug!("--write_one-- hash {:x}", hash);
        // log::debug!("--write_one-- data_offset {}", self.data_offset);
        // log::debug!("--write_one-- data_len {}", data_len);
        // log::debug!("--write_one-- item {:?}", item);

        lookup_file.write_all(&dtype.to_u8().to_le_bytes())?;
        lookup_file.write_all(&hash.to_le_bytes())?;
        lookup_file.write_all(&self.data_offset.to_le_bytes())?;
        lookup_file.write_all(&data_len.to_le_bytes())?;

        data_file.write_all(&buf)?;
        self.data_offset += data_len;

        Ok(())
    }

    /*
    pub fn write_all<S: Serialize>(&mut self, data: HashMap<u128, S>) -> Result<(), OxenError> {
        if self.read_only {
            return Err(OxenError::basic_str("Cannot write to read-only db"));
        }

        let Some(lookup_file) = self.lookup_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open before writing"));
        };
        let Some(data_file) = self.data_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open before writing"));
        };

        // Write the size of the data
        let size = data.len() as u64;
        lookup_file.write_all(&size.to_le_bytes())?;

        let mut data_offset: u64 = 0;
        // For each item,
        // write the hash,data-offset,data-len to the lookup table
        // then write the data to the data table
        for (hash, item) in data {
            let mut buf = Vec::new();
            item.serialize(&mut Serializer::new(&mut buf)).unwrap();

            let data_len = buf.len() as u64;
            lookup_file.write_all(&hash.to_le_bytes())?;
            lookup_file.write_all(&data_offset.to_le_bytes())?;
            lookup_file.write_all(&data_len.to_le_bytes())?;
            panic!("abstract me out and write the dtype")
            data_file.write_all(&buf)?;
            data_offset += data_len;
        }

        Ok(())
    }
    */

    pub fn get<D>(&self, hash: u128) -> Result<D, OxenError>
    where
        D: de::DeserializeOwned,
    {
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(OxenError::basic_str("Must call open before reading"));
        };

        let Some(mut data_file) = self.data_file.as_ref() else {
            return Err(OxenError::basic_str("Must call open before writing"));
        };

        // Find the offset and length of the data
        let Some(offset) = lookup.offsets.get(&hash) else {
            return Err(OxenError::basic_str("Cannot find hash in merkle node db"));
        };

        // Read from the data table at the offset
        // Allocate the exact amount of data
        let mut data = vec![0; offset.2 as usize];
        data_file.seek(SeekFrom::Start(offset.1))?;
        data_file.read_exact(&mut data)?;

        let val: D = rmp_serde::from_slice(&data).map_err(|e| {
            OxenError::basic_str(format!(
                "MerkleNodeDB.get({}): Error deserializing data: {:?}",
                hash, e
            ))
        })?;
        Ok(val)
    }

    pub fn map(&mut self) -> Result<HashMap<u128, CommitMerkleTreeNode>, OxenError> {
        // log::debug!("Loading merkle node db map");
        let Some(lookup) = self.lookup.as_ref() else {
            return Err(OxenError::basic_str("Must call open before reading"));
        };
        let Some(data_file) = self.data_file.as_mut() else {
            return Err(OxenError::basic_str("Must call open before writing"));
        };

        let mut file_data = Vec::new();
        data_file.read_to_end(&mut file_data)?;
        // log::debug!("Loading merkle node db map got {} bytes", file_data.len());

        let mut ret: HashMap<u128, CommitMerkleTreeNode> = HashMap::new();
        ret.reserve(lookup.num_children as usize);

        let mut cursor = std::io::Cursor::new(file_data);
        // Iterate over offsets and read the data
        for (hash, (dtype, offset, len)) in lookup.offsets.iter() {
            // log::debug!("Loading dtype {:?}", MerkleTreeNodeType::from_u8(*dtype));
            // log::debug!("Loading offset {}", offset);
            // log::debug!("Loading len {}", len);
            cursor.seek(SeekFrom::Start(*offset))?;
            let mut data = vec![0; *len as usize];
            cursor.read_exact(&mut data)?;
            let dtype = MerkleTreeNodeType::from_u8(*dtype);
            let node = CommitMerkleTreeNode {
                hash: format!("{hash:x}"),
                dtype,
                data,
                children: HashSet::new(),
            };
            // log::debug!("Loaded node {:?}", node);
            ret.insert(*hash, node);
        }

        Ok(ret)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    use crate::test;

    #[test]
    fn test_merkle_node_db() -> Result<(), OxenError> {
        test::run_empty_dir_test(|dir| {
            /*
            let mut writer_db = MerkleNodeDB::open_read_write(dir)?;
            writer_db.write_meta(".", MerkleTreeNodeType::Dir, 2)?;

            let node_1 = DirNode {
                path: "test".to_string(),
            };
            writer_db.write_one(1234, MerkleTreeNodeType::Dir, &node_1)?;

            let node_2 = DirNode {
                path: "image".to_string(),
            };
            writer_db.write_one(5678, MerkleTreeNodeType::Dir, &node_2)?;
            writer_db.close()?;

            let reader_db = MerkleNodeDB::open_read_only(dir)?;

            let size = reader_db.num_children();
            assert_eq!(size, 2);

            let data: DirNode = reader_db.get(1234)?;
            assert_eq!(data, node_1);

            let data: DirNode = reader_db.get(5678)?;
            assert_eq!(data, node_2);
            */
            Ok(())
        })
    }
}
