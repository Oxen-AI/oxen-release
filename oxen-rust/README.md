# OxenCLI

A tool to stage, commit, and push data to our servers

## Commands

`oxen init .`

`oxen status`

`oxen list datasets`

`oxen add images/`

`oxen status`

`oxen push dataset_name`


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

## staged file

Line delimited list of files we want to commit

```
/Users/gregschoeninger/data/images/img1.png
/Users/gregschoeninger/data/images/img2.jpg
/Users/gregschoeninger/data/text/1.txt
/Users/gregschoeninger/data/text/2.txt
```

## synced file

Ordered & line delimited file of which commits have been synced. 

```
2022_02_10_3214312
2022_02_10_3214332
2022_02_10_3214541
```

## Commit file

A commit is a set of files we want to add to a dataset. 
Filename is the local timestamp the commit was made. 

2022_02_10_3214312
```
424582A0F3E753A9453BFAB1A67B7F3F25392AC7546AE0FC52FBE616A89B154B
/Users/gregschoeninger/data/images/img1.png
/Users/gregschoeninger/data/images/img2.jpg
/Users/gregschoeninger/data/text/1.txt
/Users/gregschoeninger/data/text/2.txt
```