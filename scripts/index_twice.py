
from oxen import RemoteRepo
from oxen import RemoteDataset
from oxen.workspace_data_frame import index_dataset
import time

print("Creating Remote Repo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")

print("Creating Remote Dataset")
dataset = RemoteDataset(repo, "openhermes_train.parquet", index=True)

print("Indexing dataset")
index_dataset(repo, "openhermes_train.parquet")
