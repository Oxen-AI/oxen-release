
# Sample Demo Command Workflow

`oxen status`

```
fatal: no oxen repository exists, looking for directory: .oxen
```

`oxen init .`

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

`oxen checkout -b change-train`

```
create and checkout branch: change-train
```

`cp ~/Downloads/FinnSantaBarbara.jpg train/dog.1.jpg`

`oxen status`

```
On branch change-train -> 9ff8fb0d-7b8b-46ce-89da-65f059518515

Modified files:
  (use "oxen add <file>..." to update what will be committed)
  modified:  train/dog.1.jpg

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
```

`oxen add train/dog.1.jpg`

`oxen commit -m "changing train/dog.1.jpg"`

`oxen checkout main`

`oxen checkout change-train`

