# Merge Branches

Once you have added data that you are convinced improve the overall model and you want to merge the changes into the mainline there is the `oxen merge` command. In order to merge you must first checkout the branch you want to merge into. If you were following the example from [before](2_CollabAdd.md) this will be the `main` branch.

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

Let's consider a more complicated scenario. Say we have three engineers working on the same dataset. Two of them have been adding more cats and dogs to our training dataset like above, but the third is in the process of adding a `human` category. If we are following best practices, we should have two separate branches for these to features so that we can work in parallel.

The two engineers working on improving the cat vs dog accuracy might create a branch called `add-cats-and-dogs` and the other engineer might make a branch called `add-human-category`

From one workstation lets add and push the branch of more cats and dogs

```shell
$ oxen checkout -b add-cats-and-dogs
$ for i in (seq 210 220) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/dog.$i.jpg train/dog_$i.jpg ; end
$ for i in (seq 210 220) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/cat.$i.jpg train/cat_$i.jpg ; end
$ oxen add train
$ oxen commit -m "adding 20 more cats and dogs"
$ oxen push origin add-cats-and-dogs
```

Then from another lets add two images of humans, as well as modify the labels.txt file to add the human label.

```shell
$ oxen checkout -b add-human-category # create branch
$ cp ~/Downloads/human_0.jpg train/human_0.jpg # copy human image one
$ cp ~/Downloads/human_1.jpg train/human_1.jpg # copy human image two
$ echo "train/human_0.jpg 2" >> annotations/train_annotations.txt # edit annotations to include first human image
$ echo "train/human_1.jpg 2" >> annotations/train_annotations.txt # edit annotations to include second human image
$ echo "human" >> labels.txt # modify labels to include a human label
$ oxen add train # stage the changes
$ oxen commit -m "add a new category of humans, with two new training images" # commit the changes
$ oxen push origin add-human-category # push the changes to the remote
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



