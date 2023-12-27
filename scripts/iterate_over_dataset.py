
from oxen.streaming_dataset import load_dataset
from oxen.remote_repo import RemoteRepo
import os

repo = RemoteRepo("ox/gutenberg_en", host="localhost:3001")

# Make a dataset from a directory of parquet files
directory = "data_p"
# list all the files in the directory
paths = repo.ls(directory)

# prepend the directory to the paths
paths = [os.path.join(directory, path.filename) for path in paths]

# load the dataset
dataset = load_dataset(repo, paths)

print(dataset)

for i, item in enumerate(dataset):
    print(item)

    if i > 10:
        break
