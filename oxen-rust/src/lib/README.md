# 🐂 Oxen

Oxen helps you version your datasets like you version your code.

```bash
# The first step for creating any dataset should be `oxen init`

oxen init
oxen add images/
oxen commit -m "Adding my data"
oxen config --set-remote origin https://hub.oxen.ai/ox/CatDogBoundingBox
oxen push origin main
```

Clone your data faster than ever before.

```bash
# The first step in collaborating on a dataset should be `oxen clone`

oxen clone https://hub.oxen.ai/ox/CatDogBoundingBox
```

# ✅ Features

Oxen was optimized to be fast on structured and unstructured data types. Unlike traditional version control systems that are optimized for text files and code, Oxen was built from the [ground up to be fast](Performance.md) on images, video, audio, text, and more.

* 🔥 Fast (10-100x faster than existing tools)
* 🧠 Easy to learn (same commands as git)
* 🗄️ Index lots of files (millions of images? no problem)
* 🎥 Handles large files (images, videos, audio, text, parquet, arrow, json, models, etc)
* 📊 Native DataFrame processing ([oxen df](DataFrames.md) command for data exploration)
* 📈 Tracks changes over time (never worry about losing the state of your data)
* 🤝 Collaborate with your team (sync to an oxen-server)
* 👀 Better data visualization on [OxenHub](https://oxen.ai)

# Why the name Oxen?

"Oxen" 🐂 comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🌾. Let Oxen take care of the grunt work of your infrastructure so you can focus on the higher-level ML problems that matter to your product.

# Overview

No need to learn a new paradigm. 

The Oxen Command Line Interface (CLI) mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, it will be straightforward to version your datasets with Oxen.

Watch as we commit hundreds of thousands of images to an Oxen repository in a matter of seconds 🚀

## Code Entry Points

The `command`, `api`, and `model` modules are the two main entry points for the library code.

The `command` module mentally maps to the command line interface (CLI) and can be useful to string together workflows at a high level.

The `api` module consists of high level entry points for `command` module to implement the CLI. There are sub modules for working with `local` or `remote` repositories. Within local or remote there are sub modules for working with `repos`, `commits`, `branches`, etc.

The `model` module defines main data models that are used in the code base. For example the `LocalRepository`, `Commit`, and `Branch` structs.

Other core functionality is in the `core` directory for reading and writing to the oxen index. The `core` modules are used internally to the `api` and `command` modules, but should be considered private to this repo, other repos should build off `command` and `api`.

## Documentation

Watch the projects source and rebuild docs on change

`cargo install cargo-watch https`

Enable auto refresh on save of the file

`npm install -g browser-sync`

Run docs

```bash
cargo watch -s 'cargo doc && browser-sync start --port 8000 --ss target/doc -s target/doc --directory --no-open'
```

Navigate to http://localhost:8000/liboxen/