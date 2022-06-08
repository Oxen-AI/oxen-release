Move into working directory

`cd /path/to/your/dataset`

`oxen status`

```
fatal: no oxen repository exists, looking for directory: .oxen
```

*TODO* what is the command to create repo. For example `oxen create dog_classifier` would create a dir with the correct structure to add your images, and tell you how to copy into the correct spot.

`oxen init .`

*TODO* oxen create command that generates structure

```
Initial commit e4def8e8-e973-4ed4-8beb-393fde7b27b4
🐂 repository initialized at: "/Users/gregschoeninger/Datasets/SmallCatDog"
```

`oxen status`

```
On branch main -> e4def8e8-e973-4ed4-8beb-393fde7b27b4

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
  train/ with untracked 20 files
```

`oxen add train`

`oxen status`

```
On branch main -> e4def8e8-e973-4ed4-8beb-393fde7b27b4

Changes to be committed:
  added:  train/ with added 20 files

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
```

`oxen commit -m "adding train"`

```
Committing with message: adding train
```

Add the test images

`oxen add test`

Add the annotations data

`oxen add annotations`

Commit locally

`oxen commit -m "adding test data and annotations"`

View the commit history

`oxen log`

Set the remote to the server 

`oxen set-remote origin http://0.0.0.0:3000/repositories/SmallCatDog`

Push all the data

`oxen push`

