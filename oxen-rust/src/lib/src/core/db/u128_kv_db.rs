use crate::error::OxenError;

use rocksdb::{DBWithThreadMode, IteratorMode, MultiThreaded, ThreadMode};
use std::collections::HashMap;

/// More efficient than get since it does not actual deserialize the value
pub fn has_key<T: ThreadMode>(db: &DBWithThreadMode<T>, key: u128) -> bool {
    let key = key.to_be_bytes().to_vec();
    match db.get_pinned(key) {
        Ok(Some(_value)) => true,
        Ok(None) => false,
        Err(err) => {
            log::error!("Error checking for entry: {}", err);
            false
        }
    }
}

/// # Get the value from the key
pub fn get<T: ThreadMode, D>(
    db: &DBWithThreadMode<T>,
    key: u128,
) -> Result<Option<D>, OxenError>
where
    D: bytevec::ByteDecodable,
{
    let key = key.to_be_bytes().to_vec();
    match db.get(key) {
        Ok(Some(value)) => {
            // found it
            if let Ok(entry) = D::decode::<u8>(&value) {
                Ok(Some(entry))
            } else {
                Err(OxenError::basic_str(format!("Could not decode value {:?}", value)))
            }
        }
        Ok(None) => {
            // did not get val
            Ok(None)
        }
        Err(err) => {
            // error from the DB
            let err = format!(
                "Err could not fetch value from db: {} from db {:?}",
                err,
                db.path()
            );
            Err(OxenError::basic_str(err))
        }
    }
}

/// # Serializes the entry to json and writes to db
pub fn put<T: ThreadMode, D>(
    db: &DBWithThreadMode<T>,
    key: u128,
    entry: &D,
) -> Result<(), OxenError>
where
    D: bytevec::ByteEncodable + std::fmt::Debug,
{
    let key = key.to_be_bytes().to_vec();

    match entry.encode::<u8>() {
        Ok(val) => {
            db.put(key, val)?;
            Ok(())
        }
        Err(err) => {
            log::error!("Err: Could not encode value {}", err);
            Err(OxenError::basic_str("Could not encode value..."))
        }
    }
}

/*
/// # List keys and attached values
pub fn hash_map<T>(db: &DBWithThreadMode<MultiThreaded>) -> Result<HashMap<u128, T>, OxenError>
where
    T: bytevec::ByteDecodable + std::fmt::Debug,
{
    let iter = db.iterator(IteratorMode::Start);
    let mut results: HashMap<u128, T> = HashMap::new();
    for item in iter {
        match item {
            Ok((key, value)) => match (u128::from_be_bytes(&**key), T::decode::<u8>(&value)) {
                (Ok(key), Ok(value)) => {
                    let key = String::from(key);
                    results.insert(key, value);
                }
                (Ok(key), _) => {
                    log::error!("str_val_db::list() Could not values for key {}.", key)
                }
                (_, Ok(val)) => {
                    log::error!("str_val_db::list() Could not key for value {:?}.", val)
                }
                _ => {
                    log::error!("str_val_db::list() Could not decoded keys and values.")
                }
            },
            _ => {
                return Err(OxenError::basic_str(
                    "Could not read iterate over db values",
                ));
            }
        }
    }
    Ok(results)
}
*/

