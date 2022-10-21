# 🐂 oxen-release
Official repository for docs and releases of the Oxen Server and Oxen CLI.

# Overview

The Oxen CLI mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, you should be relatively comfortable versioning your datasets with Oxen.

# Installation

Navigate to [releases](https://github.com/Oxen-AI/oxen-release/releases) and download the latest version for your workstation.

OR

Install on homebrew with:

```bash
$ brew tap Oxen-AI/oxen
$ brew install oxen
```

# Basic Commands

Here is a quick refresher of common commands translated to Oxen.

## Setup User

For your commit log, you will have to setup your local Oxen user name and email

```bash
$ oxen config --name <NAME> --email <EMAIL>
```

## Create Repository

First create a new directory, navigate into it, and perform

```bash
$ oxen init .
```

## Add & Commit Data

You can stage changes that you are interested in committing with the `oxen add` command and giving a full file path or directory.

```bash
$ oxen add train/images/
$ oxen add annotations/train.csv
```

To actually commit these changes with a message you can use

```bash
$ oxen commit -m "Some informative commit message"
```

## Row Level Tracking

Oxen is smart about what file types you are adding. For example if you add a tabular data file (with an extension `.csv`, `.tsv`, `.parquet`, `.arrow`, `.jsonl`, or `.ndjson`) under the hood Oxen will index and keep track of each row.

Oxen also has some handy command line tools for working with tabular data. The `oxen df` command (short for "DataFrame") let's you easily view, modify, slice, and modify tabular data.

```bash
$ oxen df annotations/train.csv

shape: (10000, 6)
┌─────────────────────────┬───────┬────────┬────────┬────────┬────────┐
│ file                    ┆ label ┆ min_x  ┆ min_y  ┆ width  ┆ height │
│ ---                     ┆ ---   ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
│ str                     ┆ str   ┆ f64    ┆ f64    ┆ f64    ┆ f64    │
╞═════════════════════════╪═══════╪════════╪════════╪════════╪════════╡
│ images/000000128154.jpg ┆ cat   ┆ 0.0    ┆ 19.27  ┆ 130.79 ┆ 129.58 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000544590.jpg ┆ cat   ┆ 9.75   ┆ 13.49  ┆ 214.25 ┆ 188.35 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000000581.jpg ┆ dog   ┆ 49.37  ┆ 67.79  ┆ 74.29  ┆ 116.08 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000236841.jpg ┆ cat   ┆ 115.21 ┆ 96.65  ┆ 93.87  ┆ 42.29  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ ...                     ┆ ...   ┆ ...    ┆ ...    ┆ ...    ┆ ...    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000257301.jpg ┆ dog   ┆ 84.85  ┆ 161.09 ┆ 33.1   ┆ 51.26  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000130399.jpg ┆ dog   ┆ 51.63  ┆ 157.14 ┆ 53.13  ┆ 29.75  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000215471.jpg ┆ cat   ┆ 126.18 ┆ 71.95  ┆ 36.19  ┆ 47.81  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/000000251246.jpg ┆ cat   ┆ 58.23  ┆ 13.27  ┆ 90.79  ┆ 97.32  │
└─────────────────────────┴───────┴────────┴────────┴────────┴────────┘
```

To learn more about what you can do with tabular data in Oxen you can reference the documentation [here](Tabular.md)

## Setup an Oxen Server

You can either setup an Oxen Server instance yourself, or use the hosted version on [OxenHub](https://airtable.com/shril5UTTVvKVZAFE). 

To setup a local Oxen Server instance, first install the `oxen-server` binary.

Mac Installation

```bash
$ brew tap Oxen-AI/oxen
$ brew install oxen-server
```

Generate a config file that contains an access token to give it to the user to access to the server

```bash
$ oxen-server add-user --email YOUR_EMAIL --name YOUR_NAME --output user_config.toml
```

The user who needs access should copy the config to the ~/.oxen directory, which is where the Oxen CLI looks for it. If the user has not done this step, they will not have access to the server.

```bash
$ mkdir ~/.oxen
$ mv user_config.toml ~/.oxen/user_config.toml
```

Run the server

```bash
$ oxen-server start
```

The default directory that Oxen stores data is `/tmp/oxen_sync`, we definitely do not want this in production. To change it set the SYNC_DIR environment variable to a path.

```
$ export SYNC_DIR=/Path/To/Data
$ oxen-server start

Running 🐂 server on 0.0.0.0:3000
Syncing to directory: /Users/gregschoeninger/Data/oxen_server
[2022-06-08T10:00:48Z INFO  actix_server::builder] Starting 8 workers
[2022-06-08T10:00:48Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```

If you want to change the default `IP ADDRESS` and `PORT` you can do so by passing them in with the `-i` and `-p` parameters.

```bash
$ oxen-server start -i 0.0.0.0 -p 4321
```

## Use Oxen Hub

If you want to start with a remote repository that already has data, there are some that live on [Oxen](https://oxen.ai).

Message us on slack or reach out [here](https://airtable.com/shril5UTTVvKVZAFE) to get access to a private repository on [OxenHub](https://hub.oxen.ai).

Once you have access, you can add your API Key with the `oxen config` command

```bash
$ oxen config --auth-token <TOKEN>
```

To clone a repository from remote server you can use the URL supplied in [OxenHub](http://hub.oxen.ai) or your own Oxen Server instance.

```bash
$ oxen clone http://hub.oxen.ai/username/RepoName
```

## Pushing the Changes

Your changes are now saved to a commit hash locally, but if you want to share them with colleagues or the world you will have to push them to a remote.

If you have not yet set a remote, you can do so with

```bash
$ oxen remote add origin http://hub.oxen.ai/username/RepoName
```

If you had already set a remote, or cloned from a remote, simply run

```bash
$ oxen push origin main
```

You can change the remote (origin) and the branch (main) to whichever remote and branch you want to push.

## Branching

Branches are used to augment the dataset and run experiments with different subsets, transformations, or extensions of the data. The `main` branch is the default branch when you start an Oxen repository. Use different branches while you run your experiments, and when you are confident in a dataset, merge it back into the `main` branch.

You can create a new branch with

```bash
$ oxen checkout -b branch_name
```

Switch back to main

```bash
$ oxen checkout main
```

and delete the branch again

```bash
$ oxen branch -d branch_name
```

If you want to make the branch available to others, make sure to push it to a remote

```bash
$ oxen push origin branch_name
```

To see all the available branches you have locally run

```bash
$ oxen branch -a
```

## Pulling New Changes

To update your local repository to the latest changes, run

```bash
$ oxen pull origin branch_name
```

Again you can specify the remote and the branch name you would like to pull

## Merging the changes

If you feel confident in your changes, you can check out the main branch again, then merge your changes in.

```bash
$ oxen checkout main
$ oxen merge branch_name
```

If there are conflicts, Oxen will flag them and you will need to add and commit the files again in a separate commit.

```bash
$ oxen add file/with/conflict.txt
$ oxen commit -m "fixing conflict"
```

## Log

You can see the history of changes on your current branch with by running.

```bash
$ oxen log
```

## Reverting To Commit

If ever you want to go back to a point in your commit history, you can simply supply the commit id to the checkout command.

```bash
$ oxen checkout COMMIT_ID
```
