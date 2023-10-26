

<div align="center">
  <a href="https://docs.oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%93%9A-Documentation-245AF0" alt="Oxen.ai Documentation">
  </a>
  <a href="https://oxen.ai/" style="padding: 2px;">
    <img src="https://img.shields.io/badge/%F0%9F%90%82-Oxen%20Hub-245AF0" alt="Oxen.ai">
  </a>
  <a href="https://crates.io/crates/liboxen" style="padding: 2px;">
    <img src="https://img.shields.io/crates/v/liboxen.svg?color=245AF0" alt="Oxen.ai Crate"/>
  </a>
  <a href="https://pypi.org/project/oxenai/" style="padding: 2px;">
    <img src="https://img.shields.io/pypi/v/oxenai.svg?color=245AF0" alt="PyPi Latest Release"/>
  </a>
  <a href="https://discord.gg/s3tBEn7Ptg" style="padding: 2px;">
    <img src="https://dcbadge.vercel.app/api/server/8PKjB9Dz?compact=true&style=flat" alt ="Oxen.ai Discord">
  </a>
  <a href="https://twitter.com/oxen_ai" style="padding: 2px;">
    <img src="https://img.shields.io/twitter/url/https/twitter.com/oxen_ai
.svg?style=social&label=Follow%20%40Oxen.ai" alt ="Oxen.ai Twitter">
  </a>
  <br/>
</div>


# 🐂 Oxen.ai

Oxen is a lightning fast unstructured data version control system for machine learning datasets.

<p align="center">
  <img src="https://github.com/Oxen-AI/oxen-release/blob/main/images/space-ox.png?raw=true">
</p>

## 🌾 Why Build Oxen?

Oxen was build by a team of machine learning engineers, who have spent countless hours in their careers managing datasets. We have used many different tools, but none of them were as easy to use and as ergonomic as we would like. 

If you have ever tried [git lfs](https://git-lfs.com/) to version large datasets and became frustrated, we feel your pain. Solutions like git-lfs are too slow when it comes to the scale of data we need for machine learning.

If you have ever uploaded a large dataset of images, audio, video, or text to a cloud storage bucket with the name:

`s3://data/images_july_2022_final_2_no_really_final.tar.gz`

We built Oxen to be the tool we wish we had.

## 📚 Familiar Workflow

No need to learn a new paradigm.

The Oxen Command Line Interface (CLI) mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, it will be straightforward to version your datasets with Oxen.

The difference is Oxen is built for data. It is optimized to handle large files, and large datasets. It is built to be fast, and easy to use.

<p align="center">
  <a href="https://docs.oxen.ai/getting-started/intro#getting-started">🐮 Learn The Basics</a>
</p>

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/raw/main/images/cli-celeba.gif?raw=true" alt="oxen cli demo" />
</p>

## 🤖 Built for AI

If you are building an AI application, data is the lifeblood. Data is constantly changing over time, and data differentiates your model from the competition.

Whether you are building your own model from scratch, fine-tuning a pre-trained model, or using a model as a service, you will need to manage and compare the inputs and outputs over time to ensure your model is improving.

[We version our code, why not our data?](https://blog.oxen.ai/we-version-our-code-why-not-our-data/)

Versioning your data means you can experiment on models in parallel with different data. The more experiments you run, the smarter your model becomes, and more robust models lead to better products.

## ✅ Features

Oxen was optimized to be fast on structured and unstructured data types. Unlike traditional version control systems that are optimized for text files and code, Oxen was built from the [ground up to be fast](https://github.com/Oxen-AI/oxen-release/blob/main/Performance.md) on images, video, audio, text, and more.

* 🔥 Fast (10-100x faster than existing tools)
* 🧠 Easy to learn (same commands as git)
* 🗄️ Index lots of files (millions of images? no problem)
* 🎥 Handles large files (images, videos, audio, text, parquet, arrow, json, models, etc)
* 📊 Native DataFrame processing ([oxen df](https://github.com/Oxen-AI/oxen-release/blob/main/DataFrames.md) command for data exploration)
* 📈 Tracks changes over time (never worry about losing the state of your data)
* 🤝 Collaborate with your team (sync to an oxen-server)
* 🌎 [Remote Workspaces](https://docs.oxen.ai/concepts/remote-workspace) to interact with the data without downloading it
* 👀 Better data visualization on [OxenHub](https://oxen.ai)

## 🧑‍💻 Getting Started

Oxen makes versioning your datasets as easy as versioning your code. You can install through homebrew or pip or from our [releases page](https://github.com/Oxen-AI/Oxen/releases).

### 🐂 Install Command Line Tool

```bash CLI
brew tap Oxen-AI/oxen
brew install oxen
```

### 🐍 Install Python Library

```bash Python
pip install oxenai
```

### ⬇️ Clone Dataset

Clone your first Oxen repository from the [OxenHub](https://oxen.ai/explore).

<CodeGroup>

```bash CLI
oxen clone https://hub.oxen.ai/ox/CatDogBBox
```

## 🐮 Learn The Basics

To learn everything else, the full documentation can be found at [https://docs.oxen.ai](https://docs.oxen.ai).

## ⭐️ Every GitHub Star Gives an Ox its Wings

No really.

We hooked up the GitHub webhook for stars to an [OxenHub Repository](https://www.oxen.ai/ox/FlyingOxen). Learn how we did it and go find your own in our [ox/FlyingOxen](https://www.oxen.ai/ox/FlyingOxen) repository.

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/blob/main/images/ox-with-wings.png?raw=true" alt="oxen repo with wings" />
</p>

## Support

If you have any questions, comments, suggestions, or just want to get in contact with the team, feel free to email us at `hello@oxen.ai`

## Contributing

This repository contains the Python library that wraps the core Rust codebase. We would love help extending out the python interfaces, the documentation, or the core rust library.

Code bases to contribute to:

* 🦀 [Core Rust Library](https://github.com/Oxen-AI/Oxen)
* 🐍 [Python Interface](https://github.com/Oxen-AI/oxen-release/tree/main/oxen)
* 📚 [Documentation](https://github.com/Oxen-AI/docs)

If you are building anything with Oxen.ai or have any questions we would love to hear from you in our [discord](https://discord.gg/8PKjB9Dz).

## Why the name Oxen?

"Oxen" 🐂 comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🌾. Let Oxen take care of the grunt work of your infrastructure so you can focus on the higher-level ML problems that matter to your product.
