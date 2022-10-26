# 🐂 oxen-release

Oxen is command line tooling for working with large machine learning datasets 🦾. It is built from the ground up, optimized to handle many files, process data frames, and track changes in an efficient data structures.

The goal of [Oxen.ai](https://oxen.ai) is to help manage the shift from software 1.0 (writing lines of code) to [software 2.0](https://karpathy.medium.com/software-2-0-a64152b37c35) where you are managing more and more data. The `oxen` command line interface and `oxen-server` binaries are the first steps, but we plan on releasing a fully hosted Hub in the near future to help store securly your data at scale. 

Sign up [here](https://airtable.com/shril5UTTVvKVZAFE) for more information and to stay updated on the progress.

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
$ oxen init
```

## Stage Data

You can stage changes that you are interested in committing with the `oxen add` command and giving a full file path or directory.

```bash
$ oxen add images/
$ oxen add annotations/data.csv
```

## View Status

To see what data is tracked, staged, or not yet added to the repository you can use the `status` command. Note: since we are dealing with large datasets with many files, `status` rolls up the changes and summarizes them for you.

```bash
$ oxen status

On branch main -> e76dd52a4fc13a6f

Directories to be committed
  added: images with added 8108 files

Files to be committed:
  new file: images/000000000042.jpg
  new file: images/000000000074.jpg
  new file: images/000000000109.jpg
  new file: images/000000000307.jpg
  new file: images/000000000309.jpg
  new file: images/000000000394.jpg
  new file: images/000000000400.jpg
  new file: images/000000000443.jpg
  new file: images/000000000490.jpg
  new file: images/000000000575.jpg
  ... and 8098 others

Untracked Directories
  (use "oxen add <dir>..." to update what will be committed)
  annotations/ (3 items)
```

You can always paginate through the changes with the `-s` (skip) and `-l` (limit) params on the status command. Run `oxen status --help` for more info.

## Commit Changes

To commit the changes that are staged with a message you can use

```bash
$ oxen commit -m "Some informative commit message"
```

## Log

You can see the history of changes on your current branch with by running.

```bash
$ oxen log

commit 6b958e268656b0c5

Author: Ox
Date:   Fri, 21 Oct 2022 16:08:39 -0700

    adding 10,000 training images

commit e76dd52a4fc13a6f

Author: Ox
Date:   Fri, 21 Oct 2022 16:05:22 -0700

    Initialized Repo 🐂
```

## Reverting To Commit

If ever you want to go back to a point in your commit history, you can simply supply the commit id from your history to the `checkout` command.

```bash
$ oxen checkout COMMIT_ID
```

## Row Level Tracking

Oxen is smart about what file types you are adding. For example if you add a tabular data file (with an extension `.csv`, `.tsv`, `.parquet`, `.arrow`, `.jsonl`, or `.ndjson`) under the hood Oxen will index and keep track of each row.

Oxen also has some [handy command line tooling](Tabular.md) for working with tabular data. The `oxen df` command (short for "DataFrame") let's you easily view, modify, slice, and modify tabular data.

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

# Sharing Data and Collaboration

Oxen enables sharing data and collaboration between teams with `oxen-server`. Some teams setup a server instance in their local network and use it simply as backup and version control, others set it up in the cloud to enable sharing across data centers.

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

## Pushing the Changes

Once you have committed data locally, and are ready to share them with colleagues (or the world) you will have to push them to a remote.

If you have not yet set a remote, you can do so with

```bash
$ oxen remote add origin http://<YOUR_SERVER>/namespace/RepoName
```

Once a remote is set you can push

```bash
$ oxen push origin main
```

You can change the remote (origin) and the branch (main) to whichever remote and branch you want to push.

## Clone the Changes

To clone a repository from remote server you can use the URL you provided previously, and pull the changes to a new machine.

```bash
$ oxen clone http://<YOUR_SERVER>/namespace/RepoName
```

Note: Due to the potential size of data, you have to navigate into the directory, and pull the specific branch of you want.

```bash
$ cd RepoName
$ oxen pull origin main
```

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

If there are conflicts, Oxen will flag them and you will need to add and commit the files again in a separate commit. Oxen currently does not add any modifications to your working file, just flags as conflicting. If you simply want to take your version, just add and commit again.

```bash
$ oxen add file/with/conflict.jpg
$ oxen commit -m "fixing conflict"
```

## Diff

If you want to see the differences between your file and the file that is conflicting, you can use the `oxen diff` command.

Oxen knows how to compare text files as well as [tabular data](Tabular.md) between commits. Currently you must specify the specific path to the file you want to compare the changes.

If the file is tabular data `oxen diff` will show you the rows that were added or removed.

```bash
$ oxen df annotations/data.csv --add-row 'images/my_cat.jpg,cat,0,0,0,0' -o annotations/data.csv
$ oxen diff annotations/data.csv 

Added Rows

╭───────────────────┬───────┬───────┬───────┬───────┬────────╮
│ file              ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ images/my_cat.jpg ┆ cat   ┆ 0     ┆ 0     ┆ 0     ┆ 0      │
╰───────────────────┴───────┴───────┴───────┴───────┴────────╯
 1 Rows x 6 Columns
```

If the tabular data schema has changed `oxen diff` will flag and show you the columns that were added.

```bash
$ oxen df annotations/data.csv --add-col 'is_fluffy:unknown:str' -o annotations/data.csv
$ oxen diff annotations/data.csv

Added Cols
shape: (10001, 1)
┌───────────┐
│ is_fluffy │
│ ---       │
│ str       │
╞═══════════╡
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ ...       │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ unknown   │
└───────────┘


Schema has changed

Old
+------+-------+-------+-------+-------+--------+
| file | label | min_x | min_y | width | height |
| ---  | ---   | ---   | ---   | ---   | ---    |
| str  | str   | f64   | f64   | f64   | f64    |
+------+-------+-------+-------+-------+--------+

Current
+------+-------+-------+-------+-------+--------+-----------+
| file | label | min_x | min_y | width | height | is_fluffy |
| ---  | ---   | ---   | ---   | ---   | ---    | ---       |
| str  | str   | f64   | f64   | f64   | f64    | str       |
+------+-------+-------+-------+-------+--------+-----------+
```

If the file is any other type of text data, it will simply show you the added and removed lines.

```bash
$ oxen diff path/to/file.txt

 i
+here
 am a text file that
+I am modifying
-la-dee-da
+la-doo-da
+another line
```

## Dealing With Merge Conflicts

Oxen currently has three ways to deal with merge conflicts. 

1) Take the other person's changes `oxen checkout file/with/conflict.jpg --theirs`, then add and commit.
2) Take the changes in your current working directory (simply have to add and commit again)
3) Combine tabular data `oxen checkout file/with/conflict.csv --combine`

If you use the `--combine` flag, oxen will concatenate the data frames and unique them based on the row values.

## Support

If you have any questions, comments, suggestions, or just want to get in contact with the team, feel free to email us at `hello@oxen.ai`