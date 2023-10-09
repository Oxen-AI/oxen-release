

<div align="center">
  <a href="https://docs.oxen.ai/">
    <img src="https://img.shields.io/badge/%F0%9F%93%9A-Documentation-245AF0">
  </a>
  <a href="https://oxen.ai/">
    <img src="https://img.shields.io/badge/%F0%9F%90%82-Oxen%20Hub-245AF0">
  </a>
  <a href="https://crates.io/crates/liboxen">
    <img src="https://img.shields.io/crates/v/liboxen.svg?color=245AF0"/>
  </a>
  <a href="https://pypi.org/project/oxenai/">
    <img src="https://img.shields.io/pypi/v/oxenai.svg?color=245AF0" alt="PyPi Latest Release"/>
  </a>
  <br/>
</div>


# 🐂 Oxen.ai

Oxen is a lightning fast unstructured data version control system for machine learning datasets.

<p align="center">
  <img src="https://mintlify.s3-us-west-1.amazonaws.com/oxenai/images/MoonOx.png">
</p>

## 🌾 Why Build Oxen?

Oxen was build by a team of machine learning engineers, who have spent countless hours in their careers managing datasets. We have used many different tools, but none of them were as easy to use and as ergonomic as we would like. 

If you have ever tried [git lfs](https://git-lfs.com/) to version large datasets and became frustrated, we feel your pain. 

If you have ever uploaded a large dataset of images, audio, video, or text to a cloud storage bucket with the name:

`s3://data/images_july_2022_final_2_no_really_final.tar.gz`

We feel your pain. We built Oxen to be the tool we wish we had.

## 📚 Simple Learning Curve

No need to learn a new paradigm.

The Oxen Command Line Interface (CLI) mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, it will be straightforward to version your datasets with Oxen.

The difference is Oxen is built for data. It is optimized to handle large files, and large datasets. It is built to be fast, and easy to use.

<p align="center">
  <a href="#getting-started">Try It Out</a>
</p>

<p align="center">
    <img src="https://github.com/Oxen-AI/oxen-release/raw/main/images/cli-celeba.gif?raw=true" alt="oxen cli demo" />
</p>

## 🤖 Built for AI

If you are building an AI application, data is the lifeblood. Whether you are building your own model from scratch, fine-tuning a pre-trained model, or using a model as a service, you will need to manage and compare datasets over time. Data differentiates your model from the competition. 

[We version our code, why not our data?](https://blog.oxen.ai/we-version-our-code-why-not-our-data/)

Versioning your data means you can experiment on models in parallel with different data. The more experiments you run, the smarter your model becomes, and the more confident you can be in it.

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

This repository contains the documentation and release builds. To contribute to the core code base visit [https://github.com/Oxen-AI/Oxen](https://github.com/Oxen-AI/Oxen)

## Why the name Oxen?

"Oxen" 🐂 comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🌾. Let Oxen take care of the grunt work of your infrastructure so you can focus on the higher-level ML problems that matter to your product.
