# Oxen 🐂

Create a world where everyone can contribute to an Artificial General Intelligence.

# In this repository

Library, tools, and server to manage local and remote Oxen repositories.

Includes:

- `oxen` (command line interface)
- `oxen-server` (remote server to sync data to)
- `liboxen` (shared lib between cli and server)

# Build & Run

First, make sure you have Rust version **1.62** installed. You should install the Rust toolchain with rustup: https://www.rust-lang.org/tools/install.

If you are a developer and want to learn more about adding code or the overall architecture [start here](docs/dev/AddLibraryCode.md). Otherwise a quick start to make sure everything is working follows.

Build the binaries

`cargo build`

Generate a config file and token to give user access to the server

`./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output auth_config.toml`

Copy the config to the default locations

`mkdir ~/.oxen`

`mv auth_config.toml ~/.oxen/auth_config.toml`

`cp ~/.oxen/auth_config.toml data/test/config/auth_config.toml`

Run the server

`./target/debug/oxen-server start`

The default sync directory is `/tmp/oxen_sync` to change it set the SYNC_DIR environment variable to a path.

# Unit & Integration Tests

Make sure your server is running on the default port and host, then run

`cargo test`

To run with all debug output and run a specific test

`env RUST_LOG=warn,liboxen=debug,integration_test=debug cargo test -- --nocapture test_command_push_clone_pull_push`

# CLI Commands

`oxen init .`

`oxen status`

`oxen add images/`

`oxen status`

`oxen commit -m "added images"`

`oxen push origin main`

# Oxen Server

## Structure

Directories with repository names to simply sync data to, same internal file structure as your local repo

/tmp/oxen_sync
/repo_name

# APIs

Server defaults to localhost 3000

`set SERVER 0.0.0.0:3000`

You can grab your auth token from the config file above (~/.oxen/auth_config.toml)

`set TOKEN <YOUR_TOKEN>`

## List Repositories

`curl -H "Authorization: Bearer $TOKEN" "http://$SERVER/repositories"`

## Create Repository

`curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://$SERVER/repositories"`

## Add file

`curl -v -H "Authorization: Bearer $TOKEN" -X POST --data-binary @/Users/gregschoeninger/Downloads/woof_meow.jpeg "http://$SERVER/repositories/MyRepo/entries?id=1234&path=woof_meow.jpeg&is_synced=true&hash=4321&commit_id=1234&extension=jpeg"`

# Docker

Create the docker image

`docker build -t oxen/server:0.1.0 .`

Run a container on port 3000 with a local filesystem mounted from /var/oxen/data on the host to /var/oxen/data in the container.

`docker run -d -v /var/oxen/data:/var/oxen/data -p 3000:3001 --name oxen oxen/server:0.1.0`

Or use docker compose

`docker-compose up -d reverse-proxy`

`docker-compose up -d --scale oxen=4 --no-recreate`

## Local File Structure

To inspect any of the key value dbs below

`oxen inspect <PATH_TO_DB>`

```
.oxen/
  HEAD (file that contains name of current "ref")

    ex) heads/main

  refs/ (keeps track of branch heads, remote names and their current commits)
    key,value db of:

    # Local heads
    heads/main -> COMMIT_ID
    heads/feature/add_cats -> COMMIT_ID
    heads/experiment/add_dogs -> COMMIT_ID

    # What has been pushed in these branches
    remotes/experiment/add_dogs -> COMMIT_ID

  staged/ (created from `oxen add <file>` command)
    key,value db of:

    filenames -> {"hash" => "FILE_HASH", "id" => "UUID_V4"} (we generate a file ID and hash for each file that is added)
    dirnames -> count

  commits/ (created from `oxen commit -m "my message"` command. Also generates history/commit_hash)
    key,value of:

    COMMIT_HASH -> Commit

    A Commit is an object that contains, can use parent for ordering the commit logs
      - Message
      - Parent Commit ID
      - Author
      - Timestamp

  history/ (list of commits)
    COMMIT_HASH_1/
      key,value of:

      filename -> { (filename is where we copy the version back to)
        "hash" => "FILE_HASH", (use this to know if a file was different)
        ... other meta data
      }

    COMMIT_HASH_2/
    COMMIT_HASH_3/

  versions/ (copies of original files, versioned with commit ids)
    // TODO: make subdirs based on first two chars of hash, which would mean we have ~16^2=256 top level dirs, then 256 in /////       each, which would spread out the data nicely. If you take logbase 256 that means we can have a billion examples ///       split into the 4 levels easily
    //      (I think git does something somewhat similar?)
    //
    //       ex) 59E029D4812AEBF0 -> 59/E0/29D4812AEBF0
    //           72617025710EBB55 -> 72/61/7025710EBB55
    //
    // TODO: use best lossless compression type based on file type, fall back to zlib or something for rest
    // TODO: maybe create watcher program to catch and intercept on write? Is this possible?
    FILE_HASH_DIRS_1/
      COMMIT_ID_1 (dog_1.jpg)
    FILE_HASH_DIRS_2/
      COMMIT_ID_1 (dog_2.jpg)
```
