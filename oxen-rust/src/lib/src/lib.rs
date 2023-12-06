//! 🐂 liboxen
//!
//! Fast unstructured data version control.
//!
//! # Examples
//!
//! Instantiating a new repo:
//!
//! ```
//! use liboxen::command;
//!
//! // Instantiate a new repo
//! let repo = command::init("test_repo")?;
//! // Add a file to the repo
//! command::add(&repo, "file.txt")?;
//! // Commit the file
//! command::commit(&repo, "Added file.txt")?;
//!
//! ```
//!
//! Push data from local repo to remote repo:
//!
//! ```
//! use liboxen::command;
//! use liboxen::model::LocalRepository;
//!
//! // Create LocalRepository from existing repo
//! let repo = LocalRepository::new("test_repo")?;
//! // Add a file to the repo
//! command::add(&repo, "file.txt")?;
//! // Commit the file
//! command::commit(&repo, "Added file.txt")?;
//! // Set remote
//! let namespace = "ox";
//! let repo_name = "test_repo";
//! let host = "0.0.0.0:3000";
//! let remote_repo = api::remote::repositories::create(
//!     repo, namespace, repo_name, host
//! ).await?;
//! let remote_url = remote_repo.url();
//! // Set remote
//! let remote_name = "origin";
//! command::config::set_remote(&mut repo, remote_name, &remote_url)?;
//! // Push to remote
//! command::push(&repo).await?;
//! ```
//!
//! Clone data from remote url
//! ```
//! use liboxen::command;
//! use liboxen::model::LocalRepository;
//!
//! let url = "http://0.0.0.0:3000/ox/test_repo";
//! let repo_dir = "test_repo";
//! let opts = CloneOpts::new(url, &repo_dir);
//! let repo = command::clone(&opts).await?;
//! ```

extern crate approx;
extern crate bytecount;
extern crate bytesize;
// extern crate ffmpeg_next as ffmpeg;
extern crate fs_extra;
extern crate lazy_static;

pub mod api;
pub mod command;
pub mod config;
pub mod constants;
pub mod core;
pub mod error;
pub mod message;
pub mod model;
pub mod opts;
pub mod test;
pub mod util;
pub mod view;
