
# Data Point Level Version Control with Oxen

What is "data point level version control" and why does it matter?

We built Oxen from the ground up to be able to version large data sets. This includes the ability to version many files at once, as well as the ability to version the data associated with these files which we call "data point level version control".

Machine learning is all about learning a mapping from inputs to outputs. Each input and output can be thought of as a data point. Examples of inputs could be an image, a video, a piece of text, an audio clip, or just a list of numeric [features](https://en.wikipedia.org/wiki/Feature_(machine_learning)) that represent the input at a higher level. Basic outputs are usually labels during [classification](https://en.wikipedia.org/wiki/Statistical_classification) or numeric values during [regression](https://en.wikipedia.org/wiki/Regression_analysis). With the advent of [Generative AI](https://www.sequoiacap.com/article/generative-ai-a-creative-new-world/) outputs are also becoming more complex in the form of human readable text, images, videos and audio.

## Stop Using Git For Data Files

Usually version control systems track changes on a file level. The hash of the file contents determines whether this file has been changed, moved, removed, etc. 

Each file could be considered an input and an output, but there is no inherent mapping between them. A quick way to add a mapping between inputs and outputs is to store them within another file such as a csv or line delimited json file.

An example of this mapping for classifying images as cats or dogs might be a csv file that looks like this:

```
filename,label
images/00001.jpg,cat
images/00002.jpg,dog
images/00003.jpg,cat
images/00004.jpg,dog
```

At first it seems perfectly reasonable to add this file to your standard version control system. After all usually there is a `diff` command that can show you added and modified lines within a file.

The problem is unlike code, which is easy to scan line by line for changes, data that is fed into modern machine learning systems can consist of hundreds of thousands if not millions of data points. Imagine adding thousands of new rows or new columns to the your data. It quickly becomes unwieldy to debug and manage these changes with a line by line diff. We need to store these mappings in a more efficient data structure.

## Oxen DataFrames

Oxen takes version control a step further by versioning each row in structured files such as `csv`, `tsv`, `parquet`, `arrow` into an [Apache Arrow](https://arrow.apache.org/) DataFrame.

> Apache Arrow defines a language-independent columnar memory format for flat and hierarchical data, organized for efficient analytic operations on modern hardware like CPUs and GPUs. The Arrow memory format also supports zero-copy reads for lightning-fast data access without serialization overhead.

This allows Oxen to track changes between the inputs and outputs of your system, as well as fast access to any changes or subsets of your data you need for training, validation, or testing.

# CelebA Example

To illustrate the power of Oxen, let's work with the [CelebA dataset](https://mmlab.ie.cuhk.edu.hk/projects/CelebA.html). This dataset has over 200,000 images of celebrities as well as attributes about their faces. Tracking the input images alone is no small feat. If you add the images directory alone to a git repository it would quickly become apparent that the tool was not built for this use case.

Staging and committing the images in Oxen is an easy two step process.

```bash
$ oxen add images/
$ oxen commit -m "adding all the images in the dataset"
```

If you have ever tried to index hundreds of thousands of images into [git](https://git-scm.com/) or even [git lfs](https://git-lfs.github.com/) you will see Oxen is a [significant performance boost](Performance.md).

## Going Beyond Versioning Files

It quickly becomes unmanageable to have a file for each attribute, feature, or output you want to predict. This is much more suited for a DataFrame.

For example if you wanted to extend the cats vs dogs csv from above to include the bounding boxes around each individual cat and dog, you can represent the input files and outputs like this:

```
file,label,min_x,min_y,width,height
images/000000128154.jpg,cat,0.0,19.27,130.79,129.58
images/000000544590.jpg,cat,9.75,13.49,214.25,188.35
images/000000000581.jpg,dog,49.37,67.79,74.29,116.08
images/000000236841.jpg,cat,115.21,96.65,93.87,42.29
```

The CelebA dataset includes a similar mapping from inputs to outputs, but is a much more rich and full featured. It contains 202,599 rows and 41 columns of information ranging from "Bushy_Eyebrows" to "Wearing_Necklace".

A couple lines of the csv file looks like this:

```
$ head -n 3 list_attr_celeba.csv
image_id,5_o_Clock_Shadow,Arched_Eyebrows,Attractive,Bags_Under_Eyes,Bald,Bangs,Big_Lips,Big_Nose,Black_Hair,Blond_Hair,Blurry,Brown_Hair,Bushy_Eyebrows,Chubby,Double_Chin,Eyeglasses,Goatee,Gray_Hair,Heavy_Makeup,High_Cheekbones,Male,Mouth_Slightly_Open,Mustache,Narrow_Eyes,No_Beard,Oval_Face,Pale_Skin,Pointy_Nose,Receding_Hairline,Rosy_Cheeks,Sideburns,Smiling,Straight_Hair,Wavy_Hair,Wearing_Earrings,Wearing_Hat,Wearing_Lipstick,Wearing_Necklace,Wearing_Necktie,Young
000001.jpg,-1,1,1,-1,-1,-1,-1,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1,1,1,-1,1,-1,-1,1,-1,-1,1,-1,-1,-1,1,1,-1,1,-1,1,-1,-1,1
000002.jpg,-1,-1,-1,1,-1,-1,-1,1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1,-1,1,-1,1,-1,-1,1,-1,-1,-1,-1,-1,-1,1,-1,-1,-1,-1,-1,-1,-1,1
```

It is hard to make sense of this data table without opening some sort of programming environment. Oxen comes in handy for initial data exploration even before you get to versioning.

## Exploring the Data

Oxen has a convenience command `df` for exploring and transforming DataFrames. If we load the data into an Oxen DataFrame out of the gate we get a much more manageable output.

To learn more about the `df` command and common exploratory data analysis operations you might want to perform check out the [Oxen DataFrame documentation](DataFrames.md).

```bash
$ oxen df list_attr_celeba.csv
shape: (202599, 41)
┌───────┬────────────┬────────────┬──────────┬─────┬────────────┬────────────┬────────────┬───────┐
│ image ┆ 5_o_Clock_ ┆ Arched_Eye ┆ Attracti ┆ ... ┆ Wearing_Li ┆ Wearing_Ne ┆ Wearing_Ne ┆ Young │
│ _id   ┆ Shadow     ┆ brows      ┆ ve       ┆     ┆ pstick     ┆ cklace     ┆ cktie      ┆ ---   │
│ ---   ┆ ---        ┆ ---        ┆ ---      ┆     ┆ ---        ┆ ---        ┆ ---        ┆ i64   │
│ str   ┆ i64        ┆ i64        ┆ i64      ┆     ┆ i64        ┆ i64        ┆ i64        ┆       │
╞═══════╪════════════╪════════════╪══════════╪═════╪════════════╪════════════╪════════════╪═══════╡
│ 00000 ┆ -1         ┆ 1          ┆ 1        ┆ ... ┆ 1          ┆ -1         ┆ -1         ┆ 1     │
│ 1.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 00000 ┆ -1         ┆ -1         ┆ -1       ┆ ... ┆ -1         ┆ -1         ┆ -1         ┆ 1     │
│ 2.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 00000 ┆ -1         ┆ -1         ┆ -1       ┆ ... ┆ -1         ┆ -1         ┆ -1         ┆ 1     │
│ 3.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 00000 ┆ -1         ┆ -1         ┆ 1        ┆ ... ┆ 1          ┆ 1          ┆ -1         ┆ 1     │
│ 4.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ ...   ┆ ...        ┆ ...        ┆ ...      ┆ ... ┆ ...        ┆ ...        ┆ ...        ┆ ...   │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 20259 ┆ -1         ┆ -1         ┆ -1       ┆ ... ┆ -1         ┆ -1         ┆ -1         ┆ 1     │
│ 6.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 20259 ┆ -1         ┆ -1         ┆ -1       ┆ ... ┆ -1         ┆ -1         ┆ -1         ┆ 1     │
│ 7.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 20259 ┆ -1         ┆ 1          ┆ 1        ┆ ... ┆ 1          ┆ -1         ┆ -1         ┆ 1     │
│ 8.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
├╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ 20259 ┆ -1         ┆ 1          ┆ 1        ┆ ... ┆ 1          ┆ -1         ┆ -1         ┆ 1     │
│ 9.jpg ┆            ┆            ┆          ┆     ┆            ┆            ┆            ┆       │
└───────┴────────────┴────────────┴──────────┴─────┴────────────┴────────────┴────────────┴───────┘
```

## Committing DataFrames

The real power is when you stage and commit a DataFrame. Oxen processes and hashes each row and tracks the data schema over time, so that we can detect changes. Instead of seeing line by line changes on a diff, you can tell the individual rows and columns that changed. 

Committing DataFrames is no different than committing any other file. Behind the scenes Oxen works its magic to track all the changes.

Let's stage and commit the list_attr_celeba.csv file so that we have the original version tracked.

```bash
$ oxen add list_attr_celeba.csv
$ oxen commit -m "adding all attributes about the faces"
```

## Diff Changes

Then we can use the [df](DataFrames.md) command with the [--add_col](DataFrames.md#add-column) flag to project a new column onto the data for whether or not this person "Is_Famous". Everyone in this dataset is a celebrity, and by definition is famous, so we will have the default value be "1".

```bash
$ oxen df list_attr_celeba.csv --add_col 'Is_Famous:1:i64' --output list_attr_celeba.csv

....

$ oxen diff list_attr_celeba.csv

Added Columns

shape: (202599, 1)
┌───────────┐
│ Is_Famous │
│ ---       │
│ i64       │
╞═══════════╡
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ ...       │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
└───────────┘
```

We can see that Oxen is taking advantage of the structure of the DataFrame and just returning the columns that were added during the diff.

Imagine for the specific application we are working on we actually just care about a few of the attributes in the table. To view all the columns in a non-collapsed view you can use the [--schema](DataFrames.md#view-schema) flag on the [df](DataFrames.md) command.

```
$ oxen df list_attr_celeba.csv --schema

+---------------------+-------+
| column              | dtype |
+=============================+
| image_id            | str   |
|---------------------+-------|
| 5_o_Clock_Shadow    | i64   |
|---------------------+-------|

......
I am collapsing, but feel free
to try in your terminal
......

|---------------------+-------|
| Wearing_Necktie     | i64   |
|---------------------+-------|
| Young               | i64   |
|---------------------+-------|
| Is_Famous           | i64   |
+---------------------+-------+
```

Then let's narrow this down to the `image_id`, whether they are `Smiling`, and our new `Is_Famous` column and overwrite the file with the output.

```bash
$ oxen df list_attr_celeba.csv --columns 'image_id,Smiling,Is_Famous' --output list_attr_celeba.csv

shape: (202599, 3)
┌────────────┬─────────┬───────────┐
│ image_id   ┆ Smiling ┆ Is_Famous │
│ ---        ┆ ---     ┆ ---       │
│ str        ┆ i64     ┆ i64       │
╞════════════╪═════════╪═══════════╡
│ 000001.jpg ┆ 1       ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 000002.jpg ┆ 1       ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 000003.jpg ┆ -1      ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 000004.jpg ┆ -1      ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ ...        ┆ ...     ┆ ...       │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 202596.jpg ┆ 1       ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 202597.jpg ┆ 1       ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 202598.jpg ┆ 1       ┆ 1         │
├╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌┤
│ 202599.jpg ┆ -1      ┆ 1         │
└────────────┴─────────┴───────────┘
```

If we use the `oxen diff` command again we will see there is 1 added column, and 39 removed.

```bash
$ oxen diff list_attr_celeba.csv
Added Columns

shape: (202599, 1)
┌───────────┐
│ Is_Famous │
│ ---       │
│ i64       │
╞═══════════╡
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ ...       │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
├╌╌╌╌╌╌╌╌╌╌╌┤
│ 1         │
└───────────┘


Removed Columns

shape: (202599, 39)
┌─────┬─────┬────────────┬─────┬─────┬─────┬─────┬─────┬───────┐
│ 5_o ┆ Arc ┆ Attractive ┆ Bag ┆ ... ┆ Wea ┆ Wea ┆ Wea ┆ Young │
│ _Cl ┆ hed ┆ ---        ┆ s_U ┆     ┆ rin ┆ rin ┆ rin ┆ ---   │
│ ock ┆ _Ey ┆ i64        ┆ nde ┆     ┆ g_L ┆ g_N ┆ g_N ┆ i64   │
│ _Sh ┆ ebr ┆            ┆ r_E ┆     ┆ ips ┆ eck ┆ eck ┆       │
│ ado ┆ ows ┆            ┆ yes ┆     ┆ tic ┆ lac ┆ tie ┆       │
│ w   ┆ --- ┆            ┆ --- ┆     ┆ k   ┆ e   ┆ --- ┆       │
│ --- ┆ i64 ┆            ┆ i64 ┆     ┆ --- ┆ --- ┆ i64 ┆       │
│ i64 ┆     ┆            ┆     ┆     ┆ i64 ┆ i64 ┆     ┆       │
╞═════╪═════╪════════════╪═════╪═════╪═════╪═════╪═════╪═══════╡
│ -1  ┆ 1   ┆ 1          ┆ -1  ┆ ... ┆ 1   ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ -1  ┆ -1         ┆ 1   ┆ ... ┆ -1  ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ -1  ┆ -1         ┆ -1  ┆ ... ┆ -1  ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ -1  ┆ 1          ┆ -1  ┆ ... ┆ 1   ┆ 1   ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ ... ┆ ... ┆ ...        ┆ ... ┆ ... ┆ ... ┆ ... ┆ ... ┆ ...   │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ -1  ┆ -1         ┆ -1  ┆ ... ┆ -1  ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ -1  ┆ -1         ┆ -1  ┆ ... ┆ -1  ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ 1   ┆ 1          ┆ -1  ┆ ... ┆ 1   ┆ -1  ┆ -1  ┆ 1     │
├╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌┼╌╌╌╌╌╌╌┤
│ -1  ┆ 1   ┆ 1          ┆ -1  ┆ ... ┆ 1   ┆ -1  ┆ -1  ┆ 1     │
└─────┴─────┴────────────┴─────┴─────┴─────┴─────┴─────┴───────┘
```

Note even though the [--output](DataFrames.md#output-data-formats) flag has overwritten our initial data, we have nothing to worry about since we already versioned our data. With Oxen we can revert to the original at any time 😄.

Hopefully you can see that taking advantage of the innate structure of the data is already a better option than treating it like code, and sifting line by line through `git diff`. This is just one of many advantages you will see by using Oxen.

Next up see the power of [Oxen Indices](Indices.md) for fast access to your data.
