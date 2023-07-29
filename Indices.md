# Oxen Schemas & Indices

Indexing is a powerful tool within your Oxen toolchain. Oxen combines the concepts of DataFrames, Schemas, and Indices to allow you to quickly explore to subsets of your data at a specific point in time.

If you have not read about Oxen DataFrames, it would be good to read up on [Data Point Level Version Control](DataPointLevelVersionControl.md) as well as [DataFrames](DataFrames.md) before continuing.

⛔️ 👷 Caution, these features are still in development.

# Schemas

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

You'll see that each schema has a hash that is generated from the combination of its field names and data types.

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

# Indices

Indices are a useful construct in Oxen to allow constant time O(1) access to your data. This can become crucial when datasets increase in size to millions if not billions of entries.

One way of searching and finding a subset of a DataFrame is the [--filter](DataFrames.md#filter-rows) option on the [df](DataFrames.md) command. This works fine for relatively small datasets, but at the end of the day filter scans the entire column of data to find what you need, and can be quite slow to scan larger datasets.

If you are willing to put some upfront cost in indexing on commit, Oxen has the ability to lookup data in O(1) constant time.

Once you create an index on a repository, it will be updated every time you make a new commit with new data.

## Primary Key Indices

Since by default Oxen has no context about the DataFrames or the fields being ingested, the first use case for user generated indices is to create constant time access for an internal id in your system to a versioned row in Oxen.

Maybe you know what the primary key of this data should be, or you simply have more information stored in an external database that you'll want to map to later.

Indices can be added with the `schema create_index` subcommand by providing the field name you want to index.

```
$ oxen schema create_index my_schema --field 'my_id'
```

You can then quickly get back to any row within that schema that contains a specific ID value

```
$ oxen schema query my_schema --query 'my_id=1234'
```

You can also see the state of the row at a specific commit id by passing the `--source` option. Source can be a commit id or a branch name. This helps build tools to see how the data points evolves over time.

```
$ oxen schema query my_schema --query 'my_id=1234' --source $COMMIT_ID
```

If you want to see all the indices that exist within a schema, you can run the `schema indices` subcommand.

```
$ oxen schema indices my_schema
```

TODO: To delete an index within a schema, you can run the `schema delete_index` subcommand.

```
$ oxen schema delete_index my_schema --field 'my_id'
```


## Train/Test/Val Indices

Primary key lookups are one example use case for indices. Another example is to split your data into subsets. Indexed values do not have to be unique.

You may want a field that indicates whether this example belongs to train, test, or the validation set of the data. Then quickly pull the data from just the evaluation set.

```
$ oxen schema create_index bounding_box --field 'eval_set'
```

To get a quick summary of the distribution of values Oxen indexed you can run a `--query` with the `count()` function.

```
$ oxen schema query bounding_box --query 'count(eval_set)'

shape: (3, 2)
┌───────────┬───────────────────┐
│ eval_set  ┆ count             │
│ ---       ┆ ---               │
│ i64       ┆ u32               │
╞═══════════╪═══════════════════╡
│ train     ┆ 162770            │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ test      ┆ 19867             │
├╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ valid     ┆ 19962             │
└───────────┴───────────────────┘
```

To grab the subset of data that applies to a indexed subset, pass in an `=` expression.

```
$ oxen schema query bounding_box --query 'eval_set=train'

# TODO: implement
```


TODO: Show how you can push, and access these subsets from a remote server (pull, fetch with apache arrow, etc)


## File Path Indices

You may want an index on a field that contains "file" path that references some external data in the system.

If you create an index with the `file` field, OxenHub will know to map this to a file in the repository and try to render the data in place.

```
$ oxen schema create_index bounding_box --field 'file'
```

## Classification Label Indices

Another example index might be on a classification label. Say you want to be able to quickly get to all the entries that are tagged as `"person"`

```
$ oxen schema create_index bounding_box --field 'label'
$ oxen schema query bounding_box --query 'label=person'
```

Now let your imagination run wild on the type of data you work with, and how you want to version and see it evolve over time.

# Conclusion

Indexes are a powerful tool that compromise write time for constant time access to your data. They also have the benefit of being versioned so that you know exactly what the state of the index was at any given point in time. This makes time series analysis and debugging your data at any point in time much easier.

Let us know what you think or if you have any other feature requests at hello@oxen.ai.
