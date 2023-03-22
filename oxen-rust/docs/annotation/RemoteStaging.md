# Oxen Data Collection Workflow

Oxen has the concept of a remote staging area to enable easy data collection and labeling workflows. There are two main types of data one might want to stage.

1) Unstructured data files (images, videos, audio, text)
2) Structured annotations (rows for tabular data frames)

# Staging Unstructured Data

To enable collecting data without cloning an entire repository, Oxen server has the concept of a remote staging area.

You can think of this area similar to your local `oxen add` command, but the data is staged remotely.

## Example Workflow

Imagine you want an integration to collect images, review them, then add them to a commit when you are ready. One way to do this would be cloning an Oxen repo locally, downloading the images, then re-uploading them in a commit when you are ready. To avoid this extra workflow, you can POST images directly into a remote branch staging area.

Create a remote branch with the CLI

```bash 
$ oxen checkout -b add-images
$ oxen push origin add-images
```

Now that you have created a remote branch, you can use the HTTP APIs on oxen-server to upload data to a staging area on the branch. The data will not be committed until you review it and verify that you want it in the commit.

You can specify a branch and a directory you would like to upload the data to in the URI. In the example below the branch is `add-images` and the directory is `annotations`.

```
curl -X POST -H "Authorization: Bearer $TOKEN" -H "Content-Type: multipart/form-data" -F file=@/path/to/image.jpg "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/dir/add-images/images"
```

This will create a uniq file name for each file that is uploaded to avoid conflicts. It will return the file path that was created remotely.

To view the files that are staged you can simply GET the staged data on the branch `/staging/dir/add-images`

```
curl -X GET -H "Authorization: Bearer $TOKEN" "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/dir/add-images"
```

When you are ready to commit the staged data you can call the `/commit` API with the branch postfix.

```
curl -X POST -H 'Content-Type: application/json' -d '{"message": "testing committing mooooo-re data", "user": {"name": "Ox", "email": "ox@oxen.ai"}}' "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/commit/add-images"
```

# Staging Structured Data

Now that you know how to upload and stage any file to a remote staging area, you can also stage structured annotations. This is useful for collecting/extending tabular DataFrames without cloning all of the data.

Often you will have structured DataFrames that reference your unstructured data files in your repository. For example, you might have a DataFrame with a column that contains the path to the image file. You can use the `append` API to append annotations to a DataFrame.

## Example Workflow

To append to a DataFrame you must specify a branch, a file name, and a json body that represents the column values. Internally Oxen uses the DataFrame schema to convert the json to the proper csv, parquet, arrow, or jsonl format.

```
$ curl -X POST -H "Authorization: Bearer $TOKEN" -d '{"file": "images/img_1234.jpg", "label": "dog", "min_x": 100, "min_y": 50, "width": 128, "height": 112}' "http://$SERVER/api/repos/$NAMESPACE/$REPO_NAME/staging/append/add-annotations/annotations.csv" | jq

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