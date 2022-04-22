# Oxen 🐂

Libraries and tools to manage Oxen repositories.

# Components

oxen, oxen-server, liboxen

## Commands

`oxen init .`

`oxen status`

`oxen add images/`

`oxen status`

`oxen commit -m "added images"`

`oxen push`


## File Structure

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

    filenames -> ""
    dirnames -> count

  commits/ (created from `oxen commit -m "my message"` command. Also generates history/commit_hash)
    key,value of:

    COMMIT_HASH -> CommitMsg

    A CommitMsg is an object that contains, can use parent for ordering the commit logs
      - Message
      - Parent Commit ID
      - Author
      - Timestamp

  history/ (list of commits)
    COMMIT_HASH_1/
      key,value of:

      filename -> "" (if not synced)
      filename -> hash (if has been synced, since hash takes time)

    COMMIT_HASH_2/
    COMMIT_HASH_3/
  
```

# Oxen Server

## Structure

Directories with repository names to simply sync data to

## APIs

set SERVER 0.0.0.0:3000

`curl "http://$SERVER/repositories"`

```
```
