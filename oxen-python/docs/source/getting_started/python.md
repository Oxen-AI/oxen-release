# Python

The Oxen python interface makes it easy to integrate Oxen datasets directly into machine learning dataloaders or other data pipelines.

## Repositories

There are two types of repositories one can interact with, a `LocalRepo` and a `RemoteRepo`.


### Local Repo

To fully clone all the data to your local machine, you can use the `LocalRepo` class.

```python
import oxen

repo = LocalRepo("path/to/repository")
repo.clone("https://hub.oxen.ai/ox/CatDogBBox")
```

If there is a specific version of your data you want to access, you can specify the `branch` when cloning.

```python
repo.clone("https://hub.oxen.ai/ox/CatDogBBox", branch="my-pets")
```

Once you have a repository locally, you can perform the same operations you might via the command line, through the python api.

For example, you can checkout a branch, add a file, commit, and push the data to the same remote you cloned it from.

```python
import oxen

repo = LocalRepo("path/to/repository")
repo.clone("https://hub.oxen.ai/ox/CatDogBBox")
repo.checkout()
```