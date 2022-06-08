## Modify data on a local branch, and revert

Clone the repo

`oxen clone http://0.0.0.0:3000/repositories/SmallCatDog`

Pull all the data

`oxen pull`

Checkout a branch so that we can modify the data

`oxen checkout -b modify-train`

```
create and checkout branch: modify-train
```

Copy data over a different dog image

`cp ~/Downloads/FinnSantaBarbara.jpg train/dog_1.jpg`

See that the modification is detected

`oxen status`

```
On branch modify-train -> 9ff8fb0d-7b8b-46ce-89da-65f059518515

Modified files:
  (use "oxen add <file>..." to update what will be committed)
  modified:  train/dog_1.jpg

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  test/ with untracked 10 files
  annotations/ with untracked 2 files
```

Stage the modification

`oxen add train/dog_1.jpg`

TODO: Only show added and not modified in next status here

`oxen status`

Commit the modification

`oxen commit -m "changing train/dog_1.jpg to Finn"`

Revert back to main branch, see that the original still exists

`oxen checkout main`

Revert back to branch to show it reverts back

`oxen checkout modify-train`