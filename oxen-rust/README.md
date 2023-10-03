# 🐂 Oxen

Create a world where everyone can contribute to an Artificial General Intelligence, starting with the data.

# 🌾 What is Oxen?

Oxen at it's core is a data version control library, written in Rust. It's goals are to be fast, reliable, and easy to use. It's designed to be used in a variety of ways, from a simple command line tool, to a remote server to sync to, to integrations into other ecosystems such as [python](https://github.com/Oxen-AI/oxen-release).

# 📚 Documentation

The documentation for liboxen is automatically generated and uploaded to [docs.rs](https://docs.rs/liboxen/latest/liboxen/).

# 🔨 Build & Run

## Install Dependencies

Oxen is purely written in Rust 🦀. You should install the Rust toolchain with rustup: https://www.rust-lang.org/tools/install.

```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

If you are a developer and want to learn more about adding code or the overall architecture [start here](docs/dev/AddLibraryCode.md). Otherwise a quick start to make sure everything is working follows.

## Build

```
cargo build
```

If on intel mac, you may need to build with the following

```
$ rustup target install x86_64-apple-darwin
$ cargo build --target x86_64-apple-darwin
```

### Speed up the build process

You can use
the [mold](https://github.com/rui314/mold) linker to speed up builds (The
commercial Mac OS version is [sold](https://github.com/bluewhalesystems/sold)).

Assuming you have purchased a license, you can use the following instructions to
install sold and configure cargo to use it for building Oxen:

```
git clone https://github.com/bluewhalesystems/sold.git

mkdir sold/build
cd sold/build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER=c++ ..
cmake --build . -j $(nproc)
sudo cmake --install .
```

Then create `.cargo/config.toml` in your Oxen repo root with the following
content:

```
[target.x86_64-unknown-linux-gnu]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/bin/ld64.mold"]

[target.x86_64-apple-darwin]
rustflags = ["-C", "link-arg=-fuse-ld=/usr/local/bin/ld64.mold"]

```

**For macOS with Apple Silicon**, you can use the [lld](https://lld.llvm.org/) linker.

```
brew install llvm
```

Then create `.cargo/config.toml` in your Oxen repo root with the following:

```
[target.aarch64-apple-darwin]
rustflags = [ "-C", "link-arg=-fuse-ld=/opt/homebrew/opt/llvm/bin/ld64.lld", ]

```

## Run

Generate a config file and token to give user access to the server

```
./target/debug/oxen-server add-user --email ox@oxen.ai --name Ox --output user_config.toml
```

Copy the config to the default locations

```
mkdir ~/.oxen
```

```
mv user_config.toml ~/.oxen/user_config.toml
```

```
cp ~/.oxen/user_config.toml data/test/config/user_config.toml
```

Set where you want the data to be synced to. The default sync directory is `./data/` to change it set the SYNC_DIR environment variable to a path.

```
export SYNC_DIR=/path/to/sync/dir
```

Run the server

```
./target/debug/oxen-server start
```

To run the server with live reload, first install cargo-watch

```
cargo install cargo-watch
```

Then run the server like this

```
cargo watch -- cargo run --bin oxen-server start
```

# Unit & Integration Tests

Make sure your server is running on the default port and host, then run

*Note:* tests open up a lot of file handles, so limit num test threads if running everything.

You an also increase the number of open files your system allows ulimit before running tests:

```
ulimit -n 10240
```

```
cargo test -- --test-threads=$(nproc)
```

It can be faster (in terms of compilation and runtime) to run a specific test. To run a specific library test:

```
cargo test --lib test_get_metadata_text_readme
```

To run a specific integration test

```
cargo test --test test_rm test_rm_directory_restore_directory
```

To run with all debug output and run a specific test

```
env RUST_LOG=warn,liboxen=debug,integration_test=debug cargo test -- --nocapture test_command_push_clone_pull_push
```

To set a different test host you can set the `OXEN_TEST_HOST` environment variable

```
env OXEN_TEST_HOST=0.0.0.0:4000 cargo test
```

# CLI Commands

```
oxen init .
oxen status
oxen add images/
oxen status
oxen commit -m "added images"
oxen push origin main
```

# Oxen Server

## Structure

Remote repositories have the same internal structure as local ones, with the caviate that all the data is in the .oxen dir and not duplicated into a "local workspace".

# APIs

Server defaults to localhost 3000

```
set SERVER 0.0.0.0:3000
```

You can grab your auth token from the config file above (~/.oxen/user_config.toml)

```
set TOKEN <YOUR_TOKEN>
```

## List Repositories

```
curl -H "Authorization: Bearer $TOKEN" "http://$SERVER/api/repos"
```

## Create Repository

```
curl -H "Authorization: Bearer $TOKEN" -X POST -d '{"name": "MyRepo"}' "http://$SERVER/api/repos"
```

# Docker

Create the docker image

```
docker build -t oxen/server:0.6.0 .
```

Run a container on port 3000 with a local filesystem mounted from /var/oxen/data on the host to /var/oxen/data in the container.

```
docker run -d -v /var/oxen/data:/var/oxen/data -p 3000:3001 --name oxen oxen/server:0.6.0
```

Or use docker compose

```
docker-compose up -d reverse-proxy
```

```
docker-compose up -d --scale oxen=4 --no-recreate
```
