# Merge Branches

Once you have added data that you are convinced improve the overall model and you want to merge the changes into the mainline there is the `oxen merge` command. In order to merge you must first checkout the branch you want to merge into. If you were following the example from [before](2_CollabAdd.md) this will be the `main` branch.

If you weren't following the example before, you can clone and pull from this remote: 

(TODO: have server running we can pull from)

```shell
oxen clone http://0.0.0.0:3000/repositories/SmallCatDog
cd SmallCatDog
oxen pull origin main
```

Otherwise just check out the main branch.

```shell
oxen checkout main
```

To verify which branch you are on, as well as see the other branches that exist locally there is the `branch` command. To list them all use `-a`

```shell
$ oxen branch -a

* main
add-training-data
```

The asterisk next to the main branch indicates that we are on the main branch. We only have one branch that we could merge here, so let's merge it.

```shell
oxen merge add-training-data
```

Since this branch was simply a set of additions that were added after the last change to the main branch, and no one made any changes inbetween our changes, it simply will fast-forward the changes to our commit.

Assuming we don't want to use this branch anymore you can delete it with the `-d` flag

```shell
oxen branch -d add-training-data
```

Let's consider a more complicated scenario. Say we have multiple people working on the same dataset. One of them is tasked with adding a `fish` label, but the other is in the process of adding a `human` label. If we are following best practices, we should have two separate branches for these to features so that we can work in parallel.

The person working on the `fish` label might create a branch called `add-fish-label` and the other person might make a branch called `add-human-label`

From one workstation lets start by adding the `fish` label.

```shell
$ oxen checkout -b add-fish-label # create & checkout branch
$ echo "fish" >> labels.txt # append the "fish" label to our labels file
$ oxen add labels.txt # stage the labels file
$ oxen commit -m "added fish label to labels file" # commit the change
$ oxen push origin add-fish-label # push the changes
```

Then from another branch (or another workstation) let's try to add the humans label to the same file. TODO: if we have file name semantics... we can do custom merges where we could catch this scenario...?

```shell
$ oxen checkout main # Make sure we are branching from the default main branch
$ oxen checkout -b add-human-label # create & checkout branch
$ echo "human" >> labels.txt # append the "human" label to our labels file
$ oxen add labels.txt # stage the labels file
$ oxen commit -m "added human label to labels file" # commit the change
$ oxen push origin add-human-label # push the changes
```

Now we have two branches that have added different labels. Let's say that the fish branch merged in first.

```shell
$ oxen checkout main
$ oxen merge add-fish-label


```

# TODO: this is a more complicated example than we need right now...

Then add 10 images of fish from another directory. In this example the user has [tiny-imagenet](https://www.kaggle.com/datasets/akash2sharma/tiny-imagenet) downloaded to their ~/Datasets/tiny-imagenet-200/ directory.

```shell
$ for i in (seq 0 9) ; cp train/n01443537/images/n01443537_$i.JPEG train/fish_$i.jpg ; end
$ oxen add train
$ oxen commit -m "added ten images of fish to training data"
$ oxen push origin add-fish-label
```

Then from another branch lets add the humans label, as well add ten images of humans from this [human action recognition dataset](https://www.kaggle.com/datasets/meetnagadia/human-action-recognition-har-dataset).

```shell
$ oxen checkout -b add-human-label # create branch
$ cp ~/Downloads/human_0.jpg train/human_0.jpg # copy human image one
$ cp ~/Downloads/human_1.jpg train/human_1.jpg # copy human image two
$ echo "train/human_0.jpg 2" >> annotations/train_annotations.txt # edit annotations to include first human image
$ echo "train/human_1.jpg 2" >> annotations/train_annotations.txt # edit annotations to include second human image
$ echo "human" >> labels.txt # modify labels to include a human label
$ oxen add train # stage the changes
$ oxen commit -m "add a new label of humans, with two new training images" # commit the changes
$ oxen push origin add-human-label # push the changes to the remote
```

We have run some tests on the cats and dogs branch and want to merge it in before we take on the challenge of adding more categories. To simply merge the cats and dogs in is simple, it's the same workflow we did previously.

First make sure we have all the changes locally, then checkout the main branch. 

```shell
$ oxen pull origin add-cats-and-dogs
$ oxen checkout main
```

Then merge the `add-cats-and-dogs` branch

```shell
oxen merge add-cats-and-dogs
```



