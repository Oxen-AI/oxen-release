
- ask him his background
    - what sorts of projects has he been working on
- our background
    - how we got to problem
    - alchemy, watson, fyter
- team
    - former watson engineers, front end guru
- pitch
    git+github for sensory data
    first stop for
    - Discover
        - Easier to find initial data
        - Easy to see data distribution
    - Manage
        - Central hosting, scaling, pay for what you need
        - Branching, commits
    - Collaborate
        - Contribute with pull requests, merges
        - Data engineer, ML engineer, product manager
    - Audit
        - Know who added what, when, how it contributed to the model
- customers we're talking to
    - universities
    - friendlies
        - real estate
        - woebot
    - sports teams
- demo
    - clone
    - checkout branches
    - performance
    - training scripts are separate
        - clone, checkout, run train
        - for example here it is in DVC, Git LFS (two bigger competitors)
- asks
    - where does he store data
    - doing any other tutorials
    - know other educators?
    - any interesting datasets



- Feedback
    - Verified source of data
    - Post accuracy numbers
    - Kaggle with collaboration
    - Open source
    - 


# Oxen Experiment Workflow

# Want to build a POC

Put ourselves in the shoes of the Magic who wants to build a POC

# Find a dataset

We have a dataset of videos for basketball action recognition with this distribution of ~2 second videos

0 block
1 pass
2 run
3 dribble
4 shoot
5 ballinhand
6 defense
7 pick
8 noaction
9 walk

# Explore the dataset

Explored the dataset locally. I don't know the data format, distribution, etc...

Figured out it was all mp4 files and was this distribution of videos per category that we were interested in.

Every open source repo has some version of this, I have to go explore.

dataset/
    annotations.json
    labels.json
    examples/
        0000.mp4
        0000.npy
        0001.mp4
        0001.npy

6490 noaction
3490 dribble
1070 pass
 996 block
 426 shoot

# Formulate Experiments

300 images per cat (first frame)

900 images per cat (first, middle, last frame)

4800 images per cat (all frames from video) 

# Where Oxen comes in

- Add all the raw data
    - add all videos
- Create 3 branches of the data
    - split up videos into images, and train and test set annotations
- Push to Oxen to explore

# Run Models

---- experiment/300-per-class ----
Accuracy = 0.63
Precision = 0.8575197889182058
Recall = 0.5070202808112324

---- experiment/900-per-class ----
Accuracy = 0.738
Precision = 0.8556231003039514
Recall = 0.7712328767123288

---- experiment/4800-per-class ----
Accuracy = 0.744
Precision = 0.8907014681892332
Recall = 0.7428571428571429

# Conclusion

We now have a pretty good sense of precision, recall, accuracy curves given the amount of data. Want to invest more in building dataset, and adding more data.

# Benefits

Discover
    - Easier to find initial data
    - Easy to see data distribution
Manage
    - Central hosting, scaling, pay for what you need
    - Branching, commits
Collaborate
    - Contribute with pull requests, merges
    - Data engineer, ML engineer, product manager
Audit
    - Know who added what, when, how it contributed to the model


# Code Demo

`python copy_spacejam.py ~/Datasets/SpaceJam/dataset/ data/BasketballActionRecognition/videos`

`ls data/BasketballActionRecognition/videos/ | cut -d '_' -f 1 | sort | uniq -c | sort -r`

6490 noaction
3490 dribble
1070 pass
 996 block
 426 shoot

`python move_test.py data/BasketballActionRecognition/videos data/BasketballActionRecognition/test 126`

Split out test set of 126 videos from each category. Move these files so that they are never in the train again.

126 noaction
126 dribble
126 block
126 pick
126 shoot

630 videos total to test

Which leaves us these training videos

6364 noaction
3364 dribble
 944 pass
 870 block
 300 shoot

`python split_train.py data/BasketballActionRecognition/videos data/BasketballActionRecognition/train 300`

`ls data/BasketballActionRecognition/train | cut -d '_' -f 1 | sort | uniq -c | sort -r`

 300 shoot
 300 pass
 300 noaction
 300 dribble
 300 block

`ls data/BasketballActionRecognition/test | cut -d '_' -f 1 | sort | uniq -c | sort -r`
 126 shoot
 126 pass
 126 noaction
 126 dribble
 126 block

Init oxen repo, and add videos named with categories

`cd data/BasketballActionRecognition/`

`oxen init .`

`oxen add .`

`oxen commit -m "adding initial videos"`

videos/
    train/
        block_1.mp4
        block_2.mp4
        ....
        shoot_299.mp4
        shoot_300.mp4
    test/
        ....

## Experiment 1

`cd ../../`

`python take_frames_from_video.py data/BasketballActionRecognition/ first`

Start small take first frame of each video

`ls data/BasketballActionRecognition/images/train | cut -d '_' -f 1 | sort | uniq -c | sort -r`

`cd data/BasketballActionRecognition`

`oxen status`

On branch main -> ab28049e028d2a00

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  images/ with untracked 2133 files

`oxen add images`

`oxen commit -m "adding images"`

`oxen create-remote hub.oxen.ai`

`oxen remote add origin http://hub.oxen.ai/repositories/BasketballActionRecognition`

## Experiment 2

`oxen checkout -b experiment/first-middle-last-frame`

`cd ../../`

`python take_frames_from_video.py data/BasketballActionRecognition/ first_mid_last`

`cd data/BasketballActionRecognition`

```
$ oxen status
On branch experiment/first-middle-last-frame -> b627c02771a30b62

Modified files:
  (use "oxen add <file>..." to update what will be committed)
  modified:  images/labels/labels.txt
  modified:  images/annotations/train_annotations.txt
  modified:  images/annotations/test_annotations.txt

Untracked files:
  (use "oxen add <file>..." to update what will be committed)
  images/ with untracked 4260 files
```

`oxen add images`

`oxen commit -m "adding first middle and last frames"`

`oxen checkout main`

Show we are back to orig setup

`ls images/train/ | cut -d '_' -f 1 | sort | uniq -c | sort -r`

Take first, middle, last frame of every video

900 noaction
900 pass
900 block
900 pick
900 shoot

4500 images

## Experiment 3

Checkout another branch to add all the data

`oxen checkout -b experiment/all-frames`

`cd ../../`

`python take_frames_from_video.py data/BasketballActionRecognition/ all`

`oxen status`

`oxen add images`

`oxen commit -m "adding all frames"`

Take all frames of video

5854 block
5919 dribble
6031 noaction
6064 pass
6207 shoot

36000 images

-------------------------

KNOWN BUGS

Cleaning up
- checkout branch
- add & commit new directory of data
- checkout previous commit without that directory
- directory is not removed

Pushing
- Looks like we are pushing data twice when not needed
```bash
$ oxen add train/
$ oxen commit -m "adding train"
$ oxen add test/
$ oxen commit -m "adding test"
```

Failed Push, or corrupted data on server
- If one or more files fail on the push, or the server isn't synced, we should compute hash on server and repush

PERFORMANCE

- SCP
- Weights & Biases
- DVC
- Git LFS

