# Modify data on a local branch, and revert

Unzip all the data we want to work with, and change to make it the working directory.

```shell
unzip SmallCatDog.zip
cd SmallCatDog
```

Initialize the oxen repository.

```shell
oxen init .
```

Add and commit a directory.

```shell
$ oxen add train
$ oxen commit -m "adding all data"
```

Checkout a branch so that we can modify the data.

```shell
$ oxen checkout -b modify-train

create and checkout branch: modify-train
```

Copy data over a different dog image.

```shell
cp ~/Downloads/FinnSantaBarbara.jpg train/dog_1.jpg
```

See that the modification is detected.

```shell
$ oxen status

On branch modify-train -> 9ff8fb0d-7b8b-46ce-89da-65f059518515

Modified files:
  (use "oxen add <file>..." to update what will be committed)
  modified:  train/dog_1.jpg
```

Stage the modification.

```shell
oxen add train/dog_1.jpg
```

Commit the modification.

```shell
oxen commit -m "changing train/dog_1.jpg to Finn"
```

Revert back to main branch, see that the original still exists.

```shell
oxen checkout main
```

Revert back to branch to show it reverts back.

```shell
oxen checkout modify-train
```