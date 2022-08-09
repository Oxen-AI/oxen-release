# 🐂 oxen-release
Official repository for docs and releases of the Oxen Server and Oxen CLI.

# Overview

The Oxen CLI mirrors [git](https://git-scm.com/) in many ways, so if you are comfortable versioning code with git, you should be relatively comfortable versioning your datasets with Oxen.

# Installation

Navigate to [releases](https://github.com/Oxen-AI/oxen-release/releases) and download the latest version for your workstation.

# Basic Commands

Here is a quick refresher of common commands translated to Oxen.

## Create Repository

First create a new directory, navigate into it, and perform

```bash
oxen init .
```

## Checkout a Repository

If you want to grab a repository from a remote server use the URL provided by Oxen Hub, or your own Oxen Server instance.

```bash
oxen clone http://hub.oxen.ai/username/RepoName
```

## Add & Commit Data

You can stage changes that you are interested in committing with the `oxen add` command and giving a full file path or directory.

```bash
oxen add train/images/
oxen add annotations/train.tsv
```

To actually commit these changes with a message you can use

```bash
oxen commit -m "Some informative commit message"
```

## Pushing the Changes

Your changes are now saved to a commit hash locally, but if you want to share them with colleagues or the world you will have to push them to a remote.

If you have not yet set a remote, you can do so with

```bash
oxen remote add origin http://hub.oxen.ai/username/RepoName
```

If you had already set a remote, or cloned from a remote, simply run

```bash
oxen push origin main
```

You can change the remote (origin) and the branch (main) to whichever remote and branch you want to push.

## Branching

Branches are used to augment the dataset and run experiments with different subsets, transformations, or extensions of the data. The `main` branch is the default branch when you start an Oxen repository. Use different branches while you run your experiments, and when you are confident in a dataset, merge it back into the `main` branch.

You can create a new branch with

```bash
oxen checkout -b branch_name
```

Switch back to main

```bash
oxen checkout main
```

and delete the branch again

```bash
oxen branch -d branch_name
```

If you want to make the branch available to others, make sure to push it to a remote

```bash
oxen push origin branch_name
```

To see all the available branches you have locally run

```bash
oxen branch -a
```

## Pulling New Changes

To update your local repository to the latest changes, run

```bash
oxen pull origin branch_name
```

Again you can specify the remote and the branch name you would like to pull

## Merging the changes

If you feel confident in your changes, you can check out the main branch again, then merge your changes in.

```bash
oxen checkout main
oxen merge branch_name
```

If there are conflicts, Oxen will flag them and you will need to add and commit the files again in a separate commit.

```bash
oxen add file/with/conflict.txt
oxen commit -m "fixing conflict"
```

## Log

You can see the history of changes on your current branch with by running.

```bash
oxen log
```

## Reverting To Commit

If ever you want to go back to a point in your commit history, you can simply supply the commit id to the checkout command.

```bash
oxen checkout COMMIT_ID
```
