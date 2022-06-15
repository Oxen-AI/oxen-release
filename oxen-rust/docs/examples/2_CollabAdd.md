# Remote Collaboration

You should be comfortable with initializing a repository, staging data, and committing data by this point. If not you can refer to [this tutorial](1_InitAndCommit.md).

Now it is time to collaborate on a dataset with a remote server in the middle. We will assume no remote repository exists at the start. To create a remote repository you can use the `oxen create-remote` command. This will take the current working directory name and return a URL that we can use to sync the data to.

```shell
$ oxen create-remote
Remote url: http://0.0.0.0:3000/repositories/SmallCatDog
```

Now let's set a remote named `origin` to this url. You can have multiple remotes with different URLs if you want to sync to different servers. For now we will just be working with `origin`

*TODO* other commands for `oxen remote`. See `git remote --help` for examples.

```shell
$ oxen set-remote origin http://0.0.0.0:3000/repositories/SmallCatDog
```

Next let's push the data that is committed on the `main` branch to the remote named `origin`.

```shell
oxen push origin main

🐂 Oxen push origin main
Compressing commit a939e7de-e5b3-47c8-bd19-572d895e36f1
Syncing commit a939e7de-e5b3-47c8-bd19-572d895e36f1...
Compressing commit 4bd3741d-9a7a-4c5f-9fe4-dbdce1c2504c
Syncing commit 4bd3741d-9a7a-4c5f-9fe4-dbdce1c2504c...
🐂 push 21 files
██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████ 21/21
🐂 push 33 files
██████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████ 33/33
```

*TODO* we should only show one progress bar for unsynced files

Clone the Repository to a different workspace

```shell
$ cd /path/to/new/workspace
$ oxen clone http://0.0.0.0:3000/repositories/SmallCatDog
```

Pull the main branch

*TODO* Fix progress bar on pull, it currently shows 0/0 the whole time

```shell
oxen pull origin main
```

Create a branch for the changes we want to make

```shell
oxen checkout -b add-training-data
```

Copy more images of dogs into the train directory.

```shell
for i in (seq 200 209) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/dog.$i.jpg train/dog_$i.jpg ; end
```

TODO: do we want to show what the new files are in the dir, or to expand the status?

```shell
oxen status
```

Stage the changes

```shell
oxen add train/
```

Commit the changes

```shell
oxen commit -m "added 10 images of dogs"
```

Push the changes for the next person to pull. *TODO* Do not need progress bars for the original commits when pushing back.

```shell
oxen push origin add-training-data
```

In the other workspace, pull the branch.

TODO: Seperate pull and checkout commands, right now pull checks out...not sure if that's how git works?

```shell
$ cd /path/to/original/workspace/SmallCatDog

$ oxen pull origin add-training-data

$ oxen checkout add-training-data
```

Now there should be the new images to work with

```shell
ls train/
```

----- TUTORIAL BREAKS HERE.... ------

Run your experiment, and add more cat images to balance out the set.

```shell
for i in (seq 200 209) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/cat.$i.jpg train/cat_$i.jpg ; end
```

Stage the updated data

```shell
oxen add train/
```

Commit the data

```shell
oxen commit -m "adding 10 more images of cats to balance out"
```

Push the data

```shell
oxen push origin add-training-data
```

Switch to the other workspace, check the data, merge the data if it looks good

```shell
$ cd /path/to/original/workspace/

$ oxen pull origin add-training-data
```

