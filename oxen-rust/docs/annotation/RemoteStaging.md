# Oxen Data Collection Workflow

Oxen has the concept of a remote staging area to enable easy data collection and labeling workflows. There are two main types of data one might want to stage.

1) Unstructured data files (images, videos, audio, text)
2) Structured annotations (rows for tabular data frames)

# Staging Unstructured Data

To enable collecting data without cloning an entire repository, Oxen server has the concept of a remote staging area.

You can think of this area similar to your local `oxen add` command, but the data is staged remotely.

## Example Workflow

One problem with extending a dataset today is that you have to download the whole data repository locally to add a single data point. This is not ideal for large datasets.

To avoid this extra workflow, oxen has tools to upload files directly into a "remote staging area" that is tied to a specific branch.

To start, you can clone a repository with the `--shallow` flag. This flag downloads the metadata about the remote files, but not the files themselves.

```bash
$ oxen clone https://hub.oxen.ai/ox/CatDogBoundingBox --shallow
$ cd CatDogBoundingBox
$ ls # note that no files have been pulled, leaving your repo in a shallow state
```

Note: When you do a shallow clone, your local commands will not work until you pull the data. You can do this with the `oxen pull` command if you want to get back to a fully synced state.

After you have a shallow then you can create a local branch, and push it to the remote. Every remote branch has a remote staging area that is tied to the branch.

```bash 
$ oxen checkout -b add-images
$ oxen push origin add-images
```

Now that you have created a remote branch, you can interact with the remote staging area with the `oxen remote` sub command. The oxen remote subcommand defaults to checking the current branch you are on but on the remote server.

```bash
$ oxen remote status
```

The commands you are used to working with in your local workspace (`status`, `add`, `commit`, etc...) now work with the remote staging area. Each user's changes are sand-boxed to their own identity, so when you add and to a remote staging workspace, it will not overlap with other users.

To add a file to the remote staging area simply use `oxen remote add`. If you use a relative path to a file, oxen will add the file to the remote staging area that mirrors the directory locally.

```bash
$ mkdir my-images/ # create local dir
$ cp /path/to/image.jpg my-images/ # add image to local dir
$ oxen remote add my-images/image.jpg # upload image to remote staging area
```

If you give the a full path to an image you will also need to specify the data directory you would like to put it in with the `-d` flag.

```bash
$ oxen remote add /path/to/image.jpg -d my-images # upload image to remote staging area
```

You can now use the `oxen remote status` command to see the files that are staged on the remote branch.

```bash
$ oxen remote status
```

To remove a accidentally added file from the remote staging area you can use. If you do not pass the `--staged` flag, it will remove the actual file from the remote branch (TODO: right now the functionality only operates on staging area regardless of the  --staged flag).

```bash
$ oxen remote rm --staged my-images/image.jpg
```

When you are ready to commit the staged data you can call the `oxen remote commit` command.

```bash
$ oxen remote commit -m "adding my file without pulling the whole repo"
```

You have now committed data to the remote branch without cloning the full repo 🎉.

Note: If the remote branch cannot do a clean merge, remote commit will fail, and you will have to resolve the merge conflicts with some more advanced commands which we will cover later.

To see a list of remote commits on the branch you can use `remote log`. Your latest commit will be at the top of this list.

```bash
$ oxen remote log
```

## Staging Structured Data

It is common that you will want to tie some sort of annotation to your unstructured data. For example, you might want to label an image with a bounding box, or a video with a bounding box and a class label.

Oxen has native support for extending and managing structured DataFrames in the form of csv, jsonl, or parquet files. To interact with these files remotely you can use the `oxen remote df` command.

We will be focusing on adding data to these files, but you can also use the `oxen remote df` command to view the contents of a DataFrame with all the same parameters locally *TODO add link to df docs*.

```bash
$ oxen remote df annotations/train.csv # get a summary of the DataFrame

Full shape: (9000, 6)

Slice shape: (10, 6)
┌─────────────────────────┬────────┬───────┬────────┬────────┬────────┐
│ file                    ┆ height ┆ label ┆ min_x  ┆ min_y  ┆ width  │
│ ---                     ┆ ---    ┆ ---   ┆ ---    ┆ ---    ┆ ---    │
│ str                     ┆ f64    ┆ str   ┆ f64    ┆ f64    ┆ f64    │
╞═════════════════════════╪════════╪═══════╪════════╪════════╪════════╡
│ images/000000128154.jpg ┆ 129.58 ┆ cat   ┆ 0.0    ┆ 19.27  ┆ 130.79 │
│ images/000000544590.jpg ┆ 188.35 ┆ cat   ┆ 9.75   ┆ 13.49  ┆ 214.25 │
│ images/000000000581.jpg ┆ 116.08 ┆ dog   ┆ 49.37  ┆ 67.79  ┆ 74.29  │
│ images/000000236841.jpg ┆ 42.29  ┆ cat   ┆ 115.21 ┆ 96.65  ┆ 93.87  │
│ ...                     ┆ ...    ┆ ...   ┆ ...    ┆ ...    ┆ ...    │
│ images/000000201969.jpg ┆ 64.94  ┆ dog   ┆ 167.24 ┆ 73.99  ┆ 37.0   │
│ images/000000201969.jpg ┆ 38.95  ┆ dog   ┆ 110.81 ┆ 83.87  ┆ 18.02  │
│ images/000000201969.jpg ┆ 18.55  ┆ dog   ┆ 157.04 ┆ 133.63 ┆ 38.63  │
│ images/000000201969.jpg ┆ 71.11  ┆ dog   ┆ 97.72  ┆ 110.2  ┆ 35.9   │
└─────────────────────────┴────────┴───────┴────────┴────────┴────────┘
```

Say you want to add a bounding box annotation to this dataframe without cloning it locally. You can use the `--add-row` flag on the `oxen remote df` command to remotely stage a row on the DataFrame.

```bash
$ oxen remote df annotations/train.csv --add-row "my-images/image.jpg,dog,100,100,200,200"

shape: (1, 7)
┌──────────────────────────────────┬──────────────────────┬───────┬───────┬───────┬───────┬────────┐
│ _id                              ┆ file                 ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---                              ┆ ---                  ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str                              ┆ str                  ┆ str   ┆ f64   ┆ f64   ┆ f64   ┆ f64    │
╞══════════════════════════════════╪══════════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ 744bc2f5736472a0b8fec3339bf14615 ┆ my-images/image3.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
└──────────────────────────────────┴──────────────────────┴───────┴───────┴───────┴───────┴────────┘
```

This returns a UUID for the row that we can use as a handle to interact with the specific row in the remote staging area. To list add the added rows on the dataframe you can use the `oxen remote diff` command.

```bash
$ oxen remote diff annotations/train.csv

Added Rows

shape: (2, 7)
┌──────────────────────────────────┬──────────────────────┬───────┬───────┬───────┬───────┬────────┐
│ _id                              ┆ file                 ┆ label ┆ min_x ┆ min_y ┆ width ┆ height │
│ ---                              ┆ ---                  ┆ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---    │
│ str                              ┆ str                  ┆ str   ┆ f64   ┆ f64   ┆ f64   ┆ f64    │
╞══════════════════════════════════╪══════════════════════╪═══════╪═══════╪═══════╪═══════╪════════╡
│ 822ac1facbd79444f1f33a2a0b2f909d ┆ my-images/image2.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
│ ab8e28d66d21934f35efcb9af7ce866f ┆ my-images/image3.jpg ┆ dog   ┆ 100.0 ┆ 100.0 ┆ 200.0 ┆ 200.0  │
└──────────────────────────────────┴──────────────────────┴───────┴───────┴───────┴───────┴────────┘
```

If you want to delete a staged row, you can delete with the `--delete-row` flag and the value in the `_id` column.

```bash
oxen remote df annotations/train.csv --delete-row 822ac1facbd79444f1f33a2a0b2f909d
```

# HTTP APIS

You can also use the HTTP APIs on oxen-server to upload data to a staging area on the branch. This can be helpful to build directly into labeling workflows instead of interfacing with the CLI.

You can specify a branch and a directory you would like to upload the data to in the URI. In the example below the branch is `add-images` and the directory is `annotations`.

```
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: multipart/form-data"  -F file=@/path/to/image.jpg "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/$IDENTITY/dir/add-images/images"
```

To view the files that are staged you can simply GET the staged data on the branch `/staging/dir/add-images`

```
curl -X GET -H "Authorization: Bearer $TOKEN" "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/dir/add-images"
```

When you are ready to commit the staged data you can call the `/commit` API with the branch postfix.

```
curl -X POST -H 'Content-Type: application/json' -d '{"message": "testing committing mooooo-re data", "user": {"name": "Ox", "email": "ox@oxen.ai"}}' "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/$IDENTITY/commit/add-images"
```

# Staging Structured Data

Now that you know how to upload and stage any file to a remote staging area, you can also stage structured annotations. This is useful for collecting/extending tabular DataFrames without cloning all of the data.

Often you will have structured DataFrames that reference your unstructured data files in your repository. For example, you might have a DataFrame with a column that contains the path to the image file. You can use the `append` API to append annotations to a DataFrame.

## Example Workflow

To append to a DataFrame you must specify a branch, a file name, and a json body that represents the column values. Internally Oxen uses the DataFrame schema to convert the json to the proper csv, parquet, arrow, or jsonl format.

```
$ curl -X POST -H "Authorization: Bearer $TOKEN" -d '{"file": "images/img_1234.jpg", "label": "dog", "min_x": 100, "min_y": 50, "width": 128, "height": 112}' "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/df/add-row/add-annotations/annotations.csv" | jq

{
  "status": "success",
  "status_message": "resource_created",
  "modification": {
    "uuid": "a5770864-b895-4f53-8093-623f76b27296",
    "modification_type": "Append",
    "data": "{\"file\": \"images/img_1234.jpg\", \"label\": \"dog\", \"min_x\": 100, \"min_y\": 50, \"width\": 128, \"height\": 112}",
    "path": "annotations.csv",
    "timestamp": "2023-03-02T18:01:06.850765Z"
  }
}
```

To list the raw staged modifications to a file you can use the `/staging/file` API.

```
$ curl -X GET "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/file/add-images/annotations.csv" | jq

{
  "status": "success",
  "status_message": "resource_found",
  "modifications": [
    {
      "uuid": "a4bcd7e9-b43a-47ef-99a8-24a8bde77efa",
      "modification_type": "Append",
      "data": "{\"id\": 3, \"name\": \"adam\"}",
      "path": "annotations.csv",
      "timestamp": "2023-03-02T18:00:32.8009Z"
    },
    {
      "uuid": "a5770864-b895-4f53-8093-623f76b27296",
      "modification_type": "Append",
      "data": "{\"id\": 4, \"name\": \"Finn\"}",
      "path": "annotations.csv",
      "timestamp": "2023-03-02T18:01:06.850765Z"
    }
  ],
  "page_number": 1,
  "page_size": 5,
  "total_pages": 3,
  "total_entries": 13
}
```

To view these changes in DataFrame format you can use the `/staging/diff` API.

curl -X GET "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/diff/add-images/annotations.csv?page=3&page_size=5" | jq

```
{
  "status": "success",
  "status_message": "resource_found",
  "modifications": {
    "added": {
      "schema": {
        "name": null,
        "hash": "cb2f178d4c5aa0b12b8e262589ae6df3",
        "fields": [
          {
            "name": "id",
            "dtype": "i64"
          },
          {
            "name": "name",
            "dtype": "str"
          }
        ]
      },
      "slice_size": {
        "height": 3,
        "width": 2
      },
      "full_size": {
        "height": 13,
        "width": 2
      },
      "data": [
        {
          "id": 18,
          "name": "Finn"
        },
        {
          "id": 19,
          "name": "Finn"
        },
        {
          "id": 20,
          "name": "Finn"
        }
      ]
    }
  }
}
```
