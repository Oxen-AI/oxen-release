
## Branching

It is probably a good idea to do these changes on a new branch as we have already made a few significant changes to the data.

```bash
$ oxen checkout -b is_famous_and_smiling
```

Now let's add and commit our updated DataFrame.

```bash
$ oxen add list_attr_celeba.csv
$ oxen commit -m "removing all attributes except Smiling and Is_Famous"
```

TODO: ......


## Schemas

Oxen needs to detect changes to your data schemas over time. To see the schema that Oxen is tracking you can use the `schemas` command.

```bash
$ oxen schemas

+------+----------------------------------+-------------------------------------+
| name | hash                             | fields                              |
+===============================================================================+
| ?    | 36d0edc8779f42e30b0d630aa83bc83c | [file, label, ..., width, height]   |
|------+----------------------------------+-------------------------------------|
| ?    | 9d277b6a412ba4890265ec7d2a98e10b | [file, label, ..., height, is_cute] |
+------+----------------------------------+-------------------------------------+
```

We can see that neither of these schemas are named yet. If you want to reference a schema by name you can name it with the `schema name` sub command. This is useful for example if you are building an tool on top of Oxen with a specific schema in mind.

```bash
$ oxen schemas name 9d277b6a412ba4890265ec7d2a98e10b "my_bounding_box"
```

Knowing whether a data schema has changed is useful for making sure that there are no breaking changes in your data that could have downstream consequences. They also allow us to index all of the data into [Apache Arrow](https://arrow.apache.org/) DataFrames which will be useful down the line.

Data point level version control and schema tracking are essential building blocks that can be built upon to enable some powerful workflows.

We explore one of these workflows by [building an training data annotation tool](BuildingAnAnnotationTool.md) on top of Oxen.




