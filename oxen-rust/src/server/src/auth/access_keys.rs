
use liboxen::error::OxenError;
use liboxen::util;

use rand_core::OsRng;
use x25519_dalek::{EphemeralSecret, PublicKey};
use rocksdb::{DBWithThreadMode, LogLevel, MultiThreaded, Options};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::str;

pub const SECRET_KEY_FILENAME: &str = "SECRET_KEY_BASE";

#[derive(Debug, Serialize, Deserialize)]
struct JWTClaims {
    email: String,
}

pub struct KeyGenerator {
    sync_dir: PathBuf,
    db: DBWithThreadMode<MultiThreaded>
}

impl KeyGenerator {
    fn secret_key_path(sync_dir: &Path) -> PathBuf {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        hidden_dir.join(SECRET_KEY_FILENAME)
    }

    pub fn new(sync_dir: &Path) -> Result<KeyGenerator, OxenError> {
        let hidden_dir = util::fs::oxen_hidden_dir(sync_dir);
        if !hidden_dir.exists() {
            std::fs::create_dir_all(&hidden_dir)?;
        }

        let db_dir = hidden_dir.join("keys");
        let mut opts = Options::default();
        opts.set_log_level(LogLevel::Error);
        opts.create_if_missing(true);

        let secret_file = KeyGenerator::secret_key_path(sync_dir);
        if !secret_file.exists() {
            // Not really using this in the right context...but fine to generate random hash for now
            let secret = EphemeralSecret::new(OsRng);
            let public = PublicKey::from(&secret);
            let key = format!("{:?}", public);
            log::debug!("Got secret key: {}", key);
            util::fs::write_to_path(&secret_file, &key);
        }

        Ok(KeyGenerator {
            sync_dir: sync_dir.to_path_buf(),
            db: DBWithThreadMode::open(&opts, &db_dir)?
        })
    }

    pub fn create(&self, email: &str) -> Result<String, OxenError> {
        let user_claims = JWTClaims {
            email: email.to_owned(),
        };
        
        let secret_key = self.read_secret_key()?;
        match encode(&Header::default(), &user_claims, &EncodingKey::from_secret(secret_key.as_ref())) {
            Ok(token) => {
                // We map from token to email so that
                // when a request comes in with a token we can decode it
                // then check if the email matches
                // if the token doesn't exist, they def don't have access
                self.db.put(&token, email)?;
                Ok(token)
            },
            Err(_) => {
                let err = format!("Could not create access key for: {}", email);
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn get_email(&self, token: &str) -> Result<Option<String>, OxenError> {
        let key = token.as_bytes();
        match self.db.get(key) {
            Ok(Some(value)) => {
                let value = str::from_utf8(&*value)?;
                Ok(Some(String::from(value)))
            }
            Ok(None) => Ok(None),
            Err(err) => {
                let err = format!(
                    "Err could not red from commit db: {}",
                    err
                );
                Err(OxenError::basic_str(&err))
            }
        }
    }

    pub fn token_is_valid(&self, token: &str) -> bool {
        match self.get_email(token) {
            Ok(Some(email)) => {
                let secret = self.read_secret_key();
                if !secret.is_ok() {
                    return false;
                }
                
                let mut validator = Validation::new(Algorithm::HS256);
                validator.set_required_spec_claims(&["email"]);
                match decode::<JWTClaims>(&token, &DecodingKey::from_secret(secret.unwrap().as_ref()), &validator) {
                    Ok(token_data) => {
                        // Make sure we decoded the email is the one in our db
                        token_data.claims.email == email
                    },
                    _ => {
                        log::info!("auth token is not valid: {}", token);
                        false
                    }
                }
            },
            Ok(None) => false,
            Err(_) => {
                false
            }
        }
    }

    fn read_secret_key(&self) -> Result<String, OxenError> {
        let path = KeyGenerator::secret_key_path(&self.sync_dir);
        util::fs::read_from_path(&path)
    }
}


#[cfg(test)]
mod tests {

    use crate::auth::access_keys::KeyGenerator;
    use crate::test;
    use liboxen::error::OxenError;

    #[test]
    fn test_constructor() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen_result = KeyGenerator::new(sync_dir);
            assert!(keygen_result.is_ok());
            Ok(())
        })
    }

    #[test]
    fn test_generate_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            let token = keygen.create("g@oxen.ai")?;
            assert!(!token.is_empty());
            
            Ok(())
        })
    }

    #[test]
    fn test_generate_and_get_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            let email = "g@oxen.ai";
            let og_token = keygen.create(email)?;
            
            let fetched_email_opt = keygen.get_email(&og_token)?;
            assert!(fetched_email_opt.is_some());
            assert_eq!(email, fetched_email_opt.unwrap());
            
            Ok(())
        })
    }

    #[test]
    fn test_generate_and_validate() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            let email = "g@oxen.ai";
            let token = keygen.create(email)?;
            
            let is_valid = keygen.token_is_valid(&token);
            assert!(is_valid);
            
            Ok(())
        })
    }

    #[test]
    fn test_invalid_key() -> Result<(), OxenError> {
        test::run_empty_sync_dir_test(|sync_dir| {
            let keygen = KeyGenerator::new(sync_dir)?;
            
            let is_valid = keygen.token_is_valid("not-a-valid-key");
            assert!(!is_valid);
            
            Ok(())
        })
    }
}