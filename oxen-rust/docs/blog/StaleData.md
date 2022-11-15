# Stale data is :hankey:

All machine learning solutions start with a good dataset. Much of a machine learning engineer's job entails wrangling and maintaining data. As they say, garbage in, garbage out.

Worst case scenario they are just stored in .zip files on a local hard drive in your office. Slightly better case, the team uploaded the dataset to Dropbox or Google Drive. Better case they are dumped to a blob storage on AWS, GCP, or Azure. All three of these approaches still feel like we are in the early days of ML.

Machine learning systems should be dynamic, constantly learning, and this starts with the data that is fed to them. As ML engineers, we are replacing writing lines of code by integrating more and more data (see Andrej Karpathy's post). Yet we are still treating data like it is a static asset.

Imagine if we still wrote code by dropping sending zip files in dropbox to eachother, it would be a mess. It would take forever to debug and get a new release out. Luckily for us people invented version control systems to solve this exact problem.

At Oxen we are building data point level version control optimized for machine learning datasets. These datasets are unique because often they have many data files, of varying sizes, as well as other large data files in the form of csv, parquet or json files.

To illustrate the power of Oxen, let's start with an example dataset from [Kaggle](https://www.kaggle.com/datasets/yusufemir/lemon-quality-dataset).

## When life gives you lemons, make sure they are the right version

Imagine you want to start a lemonade stand. But not just any old lemonade stand. You are building a futuristic lemonade factory with advanded computer vision to squeeze the highest quality lemons out of the batch. By scanning and sorting every lemon on the line with a computer vision, never again will you accidently make a bad batch of lemonade.

Let's start with a simple computer vision dataset to detect the quality of lemons. Kaggle is another common starting point for ML datasets, and is where we will grab our initial lemon dataset. 

https://www.kaggle.com/datasets/yusufemir/lemon-quality-dataset

Datasets are uploaded to Kaggle in the form of zip files, and can be downloaded in the same format. Let's grab a zip file from here and transform it into a well versioned Oxen repository. 

TODO: picture of click the download button

Once you have the data downloaded, unzip it into a the working directory 

```bash
$ unzip archive.zip
$ rm archive.zip
$ cd lemons_dataset
```

First make sure you have the `oxen` client installed, you can find [installation instructions here](https://github.com/Oxen-AI/oxen-release/blob/main/Installation.md).

Oxen is command line tooling that mirrors [git](https://git-scm.com/) so that there is minimal learning curve to get up and running with an Oxen repository.

First initialize the repo with the `init` command.

```bash
$ oxen init
```

You can use the `status` command to view a summary of the files that are not yet staged.

```bash
$ oxen status
```

TODO add image

Since ML workflows typically deal with many files, Oxen rolls up summaries of directories so you can quickly see the summary.

For example if you stage the good_quality images directory with over 1,000 and run status again, you will see a nice summary in the status.

```bash
$ oxen add good_quality/
$ oxen status
```

TODO add image

You can use the `-a`, `-l`, and `-s` flags on the status command to print all, limit, or skip to files in the summary.

To commit the changes to the history, you can run the `commit` command with the `-m` flag for a message. This command will version and copy the files into the .oxen/ directory so that you can always revert to them later.

```bash
$ oxen commit -m "adding good quality lemons"
```

To add and commit the rest of the images you can run.

```bash
$ oxen add .
$ oxen commit -m "adding bad quality lemons, and background images"
```

To see the commit history simply use the `log` command.

```bash
$ oxen log
```

TODO: add image

Congratulations 


If you find this interesting tag us on Twitter at @oxendrove or reach out at hello@oxen.ai. We are also in the process of building out a web interface and Hub and would love early feedback, for access reach out [here](https://airtable.com/shril5UTTVvKVZAFE)