# 🐂 oxen-release

Oxen helps you version on your machine learning datasets like you version your code. 

In a world of [Software 2.0](https://karpathy.medium.com/software-2-0-a64152b37c35) where we are replacing lines with neural networks and large datasets, we need better tooling to keep track of changes to the data and models over time.

Versioning datasets with `git` or `git lfs` is slow and painful 😩. Git was built for code repositories, not data. Oxen is built from the ground up for speed and large datasets 🐂 💨 and is 10-100x faster than using git.

It is built from the ground up to be fast 🔥 and easy to learn 🧠

* Optimized to handle many files / large files that would be inefficient to store in git
  * Images, Videos, Audio, Text, Parquet, Json, Models etc
* Native DataFrame processing
* Tracks changes in an efficient data structure
* Mirrors git commands, easy to learn
* Helps slice dice your data to the subset you need for training, testing, and evaluation.
* CLI + Server to sync your changes between team members and other infrastructure
* Syncs to [OxenHub](https://oxen.ai) for visualization and collaboration on public and private repositories.

Sign up [here](https://airtable.com/shril5UTTVvKVZAFE) for more information and to stay updated on the progress.

# Why the name Oxen?

"Oxen" comes from the fact that the tooling will plow, maintain, and version your data like a good farmer tends to their fields 🐂 🌾. Let Oxen take care of grunt work of your infrastructure so you can focus on the higher level ML problems that matter to your product.

# Overview

The Oxen Command Line Interface (CLI) mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, it will be straight forward to version your datasets with Oxen.

# Installation

Navigate to [releases](https://github.com/Oxen-AI/oxen-release/releases) and download the latest version for your workstation or follow the [installation instructions](Installation.md) for using homebrew or the debian package.

# Basic Commands

Here is a quick overview of common commands translated to Oxen.

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
```

## View Status

To see what data is tracked, staged, or not yet added to the repository you can use the `status` command. 

Note: since we are dealing with large datasets with many files, `status` rolls up the changes and summarizes them for you.

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

If ever you want to change your working directory to a point in your commit history, you can simply supply the commit id from your history to the `checkout` command.

```bash
$ oxen checkout COMMIT_ID
```

## Restore Working Directory

The `restore` command comes in handy if you made some changes locally and you want to revert the changes. This can be used for example if you accidentally delete or modify or stage a file that you did not intend to.

```bash
$ oxen restore path/to/file.txt
```

Restore defaults to restoring the files to the current HEAD. For more detailed options, as well as how to unstage files refer to the [restore documentation](commands/Restore.md).

## Data Point Level Version Control

Oxen is smart about what file types you are adding. For example if you track a tabular data file (with an extension `.csv`, `.tsv`, `.parquet`, `.arrow`, `.jsonl`, or `.ndjson`) Oxen will index and keep track of each row of data.

```bash
$ oxen add annotations/train.csv
$ oxen commit -m "adding rows and rows of data"
```

Under the hood Oxen will detect the data schema and hash every row of content. This allows us to build a content addressable DataFrame to track the changes to the rows and columns over time. To learn more about the power of indexing DataFrames checkout the [data point level version control documentation](DataPointLevelVersionControl.md).

Oxen also has some [handy command line tooling](DataFrames.md) for [Exploratory Data Analysis](https://en.wikipedia.org/wiki/Exploratory_data_analysis) with DataFrames. The `oxen df` command let's you easily view, modify, slice, and modify the data.

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

To learn more about what you can do with tabular data in Oxen you can reference the documentation [here](DataFrames.md)

## Integrating Labeling Tools

For most supervised learning projects you will have some sort of annotation or labeling workflow. There are some popular open source tools such as [Label Studio](https://labelstud.io/) for labeling data that can integrate with an Oxen workflow. 

For an example of integrating Oxen into your Label Studio workflow, check out our [Oxen Annotation Documentation](annotation/LabelStudio.md).

## Diff

If you want to see the differences between your file and the file that is conflicting, you can use the `oxen diff` command.

Oxen knows how to compare text files as well as [tabular data](DataFrames.md) between commits. Currently you must specify the specific path to the file you want to compare the changes.

If the file is tabular data `oxen diff` will show you the rows that were added or removed.

```bash
$ oxen df annotations/data.csv --add_row 'images/my_cat.jpg,cat,0,0,0,0' -o annotations/data.csv
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
$ oxen df annotations/data.csv --add_col 'is_fluffy:unknown:str' -o annotations/data.csv
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

# Sharing Data and Collaboration

Oxen enables sharing data and collaboration between teams with `oxen-server`. Some teams setup a server instance in their local network and use it simply as backup and version control, others set it up in the cloud to enable sharing across data centers.

## Setup an Oxen Server

You can either setup an `oxen-server` instance yourself, or use the hosted version on OxenHub. To use the hosted OxenHub solution you can contact us [here](https://airtable.com/shril5UTTVvKVZAFE). 

To setup a local Oxen Server instance, first [install](Installation.md) the `oxen-server` binary.

The server can be run with access token authentication turned on or off. The server runs with no authentication by default:

```bash
$ oxen-server start
```

To enable authentication, generate a token to give it to the user to access to the server

```bash
$ oxen-server add-user --email YOUR_EMAIL --name YOUR_NAME

User access token created:

XXXXXXXX

To give user access have them run the command `oxen config --auth <HOST> <TOKEN>`
```

You may have different authentication tokens for different hosts. From the client side, you can setup an auth token per host with the `config` command. If you ever need to debug or edit the tokens manually, they are stored in the `~/.oxen/user_config.toml` file.

```bash
$ oxen config --auth <HOST> <TOKEN>
$ cat ~/.oxen/user_config.toml
```

To run the server with authentication, use the `-a` flag

```bash
$ oxen-server start -a
```

The default directory that Oxen stores data is `/tmp/oxen_sync`, we definitely do not want this in production. To change it set the SYNC_DIR environment variable to a path.

```
$ export SYNC_DIR=/Path/To/Data
$ oxen-server start -a

Running 🐂 server on 0.0.0.0:3000
Syncing to directory: /Path/To/Data
[2022-06-08T10:00:48Z INFO  actix_server::builder] Starting 8 workers
[2022-06-08T10:00:48Z INFO  actix_server::server] Actix runtime found; starting in Actix runtime
```

If you want to change the default `IP ADDRESS` and `PORT` you can do so by passing them in with the `-i` and `-p` parameters.

```bash
$ oxen-server start -i 0.0.0.0 -p 4321
```

## Pushing the Changes

Once you have committed data locally and are ready to share them with colleagues (or the world) you will have to push them to a remote.

You can either create a remote through the web UI on [OxenHub](https://oxen.ai) or if you have setup a server your self, you will have to run the `create-remote` command.

```bash
$ oxen create-remote MyNamespace MyRepoName <HOST>
```

Repositories that live on an Oxen Server have the idea of a `namespace` and a `name` to help you organize your repositories.

Once you know your remote repository URL you can add it as a remote.

```bash
$ oxen remote add origin http://<HOST>/MyNamespace/MyRepoName
```

Once a remote is set you can push

```bash
$ oxen push origin main
```

You can change the remote (origin) and the branch (main) to whichever remote and branch you want to push.

## Clone the Changes

To clone a repository from remote server you can use the URL you provided previously, and pull the changes to a new machine.

```bash
$ oxen clone http://<HOST>/MyNamespace/MyRepoName
```

Note: Due to the potential size of data, oxen does not immediately pull the data. You have to navigate into the directory, and pull the specific branch that you want.

```bash
$ cd MyRepoName
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

## Dealing With Merge Conflicts

Oxen currently has three ways to deal with merge conflicts. 

1) Take the other person's changes `oxen checkout file/with/conflict.jpg --theirs`, then add and commit.
2) Take the changes in your current working directory (simply have to add and commit again)
3) Combine tabular data `oxen checkout file/with/conflict.csv --combine`

If you use the `--combine` flag, oxen will concatenate the data frames and unique them based on the row values.

## Support

If you have any questions, comments, suggestions, or just want to get in contact with the team, feel free to email us at `hello@oxen.ai`