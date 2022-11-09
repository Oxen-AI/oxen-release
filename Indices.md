# Oxen Schemas & Indices

Indexing is a powerful tool within your Oxen toolchain. Oxen combines the concepts of DataFrames, Schemas, and Indices to allow you to quickly get to subsets of data that you want to train, evaluate, or simply explore.

If you have not read about Oxen DataFrames, it would be good to read up on the [Data Point Level Version Control documentation](DataPointLevelVersionControl.md) as well as the [DataFrame documentation](DataFrames.md) before continuing.

## Schemas

When you add and commit a DataFrame to Oxen, it will automatically track the schema associated with the file.

TODO: add command to clone data

For example let's use a subset of the MSCoco Dataset as an example that has been processed into a DataFrame.

```bash
$ oxen df processed/annotations/coco/bb_train2017.csv

shape: (860001, 6)
┌─────────────────────────────────────┬──────────────┬────────┬────────┬────────┬────────┐
│ file                                ┆ label        ┆ min_x  ┆ min_y  ┆ width  ┆ height │
│ ---                                 ┆ ---          ┆ ---    ┆ ---    ┆ ---    ┆ ---    │
│ str                                 ┆ str          ┆ f64    ┆ f64    ┆ f64    ┆ f64    │
╞═════════════════════════════════════╪══════════════╪════════╪════════╪════════╪════════╡
│ raw/images/coco/train2017/000000... ┆ motorcycle   ┆ 359.17 ┆ 146.17 ┆ 112.45 ┆ 213.57 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ person       ┆ 339.88 ┆ 22.16  ┆ 153.88 ┆ 300.73 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ person       ┆ 471.64 ┆ 172.82 ┆ 35.92  ┆ 48.1   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ bicycle      ┆ 486.01 ┆ 183.31 ┆ 30.63  ┆ 34.98  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ ...                                 ┆ ...          ┆ ...    ┆ ...    ┆ ...    ┆ ...    │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ cup          ┆ 195.73 ┆ 267.76 ┆ 13.14  ┆ 25.15  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ sink         ┆ 270.32 ┆ 260.22 ┆ 114.92 ┆ 67.4   │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ person       ┆ 474.76 ┆ 158.66 ┆ 25.24  ┆ 69.33  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
│ raw/images/coco/train2017/000000... ┆ refrigerator ┆ 105.04 ┆ 325.97 ┆ 187.84 ┆ 49.03  │
└─────────────────────────────────────┴──────────────┴────────┴────────┴────────┴────────┘
```

Add and commit the training data file.

```bash
$ oxen add processed/annotations/coco/bb_train2017.csv
$ oxen commit -m "adding training data file"
```

Then run the `schemas` command to see a list of the detected schemas.

```
$ oxen schemas

+------+----------------------------------+-----------------------------------+
| name | hash                             | fields                            |
+=============================================================================+
| ?    | 36d0edc8779f42e30b0d630aa83bc83c | [file, label, ..., width, height] |
+------+----------------------------------+-----------------------------------+
```

You'll see that each schema has a hash that is generated from the combination of it's field names and data types.

To see a more expanded view of a schema you can run the `schema show` subcommand.

```
$ oxen schemas show 36d0edc8779f42e30b0d630aa83bc83c

Schema has no name, to name run:

  oxen schemas name 36d0edc8779f42e30b0d630aa83bc83c "my_schema"

+----+--------+-------+
| id | name   | dtype |
+=====================+
| 0  | file   | str   |
|----+--------+-------|
| 1  | label  | str   |
|----+--------+-------|
| 2  | min_x  | f64   |
|----+--------+-------|
| 3  | min_y  | f64   |
|----+--------+-------|
| 4  | width  | f64   |
|----+--------+-------|
| 5  | height | f64   |
+----+--------+-------+
```

Schemas can either be referenced by their hash, or their name, to give a schema a more readable name you can use the `schema name` subcommand.

```
$ oxen schemas name 36d0edc8779f42e30b0d630aa83bc83c "bounding_box"
```

One of the benefits of having schemas versioned is it helps you know how your data changes over time. The other added benefit is indexing the data.

## Indices

Often when dealing with large datasets you want to search, filter, and get down to a subset of the data. You may want to train on this subset, evaluate on this subset, or simply fork off this portion of the data and start a new dataset.

Oxen makes this easy with the `schema index` sub command.

The first argument is the schema name or hash, and the second is an expression you want to evaluate.

This knows to go index the massive data.arrow file and run an aggregation on it based on a *column name key*

```
$ oxen schema index --schema bounding_box --expr "('label') -> "
```

Then we can quickly query this data

```
$ oxen schema query --schema -q 
```

TODO: What is the output of this command?
TODO: Can we do custom index functions? Like index and search on text? Or by embeddings?