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

      filename -> { (filename is where we hard link it back to)
        "hash" => "FILE_HASH", (use this to know what version it was, and only add new changes)
        "is_synced" => false
      }

    COMMIT_HASH_2/
    COMMIT_HASH_3/

  data/ (version copies of original files, once 10,000 is hit, we create new dir, and update all mirror paths)
    // TODO: compress based on file type, maybe create watcher program to catch and intercept on write
    FILE_UUID_1/
      COMMIT_ID_1 (dog_1.jpg)
      COMMIT_ID_2 (dog_1.jpg version 2)
    FILE_UUID_2/
      COMMIT_ID_1 (dog_2.jpg)

    TODO: Seems like we only need to update when the file changes...
          Think of best way to only make minimal # of links
          Do we have a dir that is an encoded version of the file name?
          Then keep each iteration of the file in there?
```

# Oxen Server

## Structure

Directories with repository names to simply sync data to

## APIs

set SERVER 0.0.0.0:3000

`curl "http://$SERVER/repositories"`

```
```
