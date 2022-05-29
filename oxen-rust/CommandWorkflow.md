
# Sample Demo Command Workflow

`oxen status`

TODO: what is the command to create repo

```
fatal: no oxen repository exists, looking for directory: .oxen
```

`oxen init .`

TODO: oxen create command that generates structure

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

## Demo changing data on branch

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

TODO: Only show added and not modified in next status here

`oxen status`

`oxen commit -m "changing train/dog.1.jpg to Finn"`

Revert back to main branch

`oxen checkout main`

`oxen checkout change-train`

## Add all data on main branch

`oxen checkout main`

`oxen add test`

`oxen add annotations`

`oxen commit -m "adding test data"`

## Mess with the test data to get better stats

`oxen checkout -b play-with-test-data`

`rm test/10.jpg`

`head -n 9 annotations/test_annotations.txt > annotations/test_annotations.txt`

`oxen add annotations/test_annotations.txt`

`oxen add test/10.jpg`

## Modification should show up in summary

`oxen commit -m "remove 10.jpg from test"`

Revert back

`oxen checkout main`

## Colaboration

Push the changes

`oxen set-remote origin http://0.0.0.0:3000/repositories/SmallCatDog`

`oxen push`

Pull the changes to some other workspace

`oxen clone http://0.0.0.0:3000/repositories/SmallCatDog`

`oxen pull origin change-train`

`for i in (seq 200 210) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/dog.$i.jpg train/ ; end`


## TODO: do we want to show what the new files are in the dir, or to expand the status?

`oxen status`


