
# Delete & Restore data

The purpose of this tutorial is to show you how to restore data that has been removed from a repository, given that it has been committed at some point.

Unzip sample data and initialize an oxen repository.

```shell
unzip SmallCatDog.zip
cd SmallCatDog
```

Pull all the data

```shell
oxen init .
```

```shell
$ oxen add .
$ oxen commit -m "adding all data"
```

Checkout branch so we can safely remove data

```shell
oxen checkout -b remove-data
```

Remove the file from the filesystem.

```shell
rm train/dog_1.jpg
```

Add and commit the removed file. TODO `oxen rm` command

```shell
$ oxen add train/dog_1.jpg
$ oxen commit -m "removing dog_1.jpg"
```

Checkout `main` branch to restore file

```shell
oxen checkout main
```

Checkout `remove-data` branch to remove the file again

```shell
oxen checkout remove-data
```

Remove an image file

`rm test/10.jpg`

Remove that reference from the test_annotations

`head -n 9 annotations/test_annotations.txt > annotations/test_annotations_modified.txt`

`mv annotations/test_annotations_modified.txt annotations/test_annotations.txt`

See that they have been removed in status

`oxen status`

Add the changes

`oxen add annotations/test_annotations.txt`

`oxen add test/10.jpg`

See that they show up as removed and modified in the status

`oxen status`

Commit the removals

`oxen commit -m "remove 10.jpg from test"`

Revert back to main to show that we can revert

`oxen checkout main`
