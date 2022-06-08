## Adding data workflow

Clone the Repository to a workspace

`cd /path/to/new/workspace`

`oxen clone http://0.0.0.0:3000/repositories/SmallCatDog`

Pull the main branch

*TODO* Fix progress bar on pull, it currently shows 0/0 the whole time

`oxen pull origin main`

Create a branch for the changes

`oxen checkout -b add-training-data`

Copy more images of dogs into the train directory

`for i in (seq 200 209) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/dog.$i.jpg train/dog_$i.jpg ; end`

TODO: do we want to show what the new files are in the dir, or to expand the status?

`oxen status`

Stage the changes

`oxen add train/`

Commit the changes

`oxen commit -m "added 10 images of dogs"`

Push the changes for the next person to pull

*TODO* Oxen push does not take remote and branch right now

`oxen push origin add-training-data`

In the other workspace, pull the branch

`cd /path/to/original/workspace/SmallCatDog`

`oxen pull origin add-training-data`

`oxen checkout add-training-data`

Now there should be the new images to work with

`ls train/`

Run your experiment, and add more cat images to balance out the set

`for i in (seq 200 210) ; cp ~/Datasets/DogsVsCats/dogs-vs-cats-train/cat.$i.jpg train/cat_$i.jpg ; end`

Stage the data

`oxen add train/`

Commit the data

`oxen commit -m "adding more images of cats to balance out"`

Push the data

`oxen push origin add-training-data`

Switch to the other workspace, check the data, merge the data if it looks good

`cd /path/to/original/workspace/`

`oxen pull origin add-training-data`

