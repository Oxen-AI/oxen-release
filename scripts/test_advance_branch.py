from oxen import RemoteRepo
from oxen import RemoteDataset
import openai
import tqdm
from oxen.data_frame import index_dataset

print("Creating Remote Repo")
repo = RemoteRepo("ox/Testing123", "staging.hub.oxen.ai")

files = repo.ls()
print("Files")
for file in files:
    print(f"--{file}")

print("Creating Remote Dataset")

# Connect to the dataset
dataset = RemoteDataset(repo, "data.csv")


num_pages = dataset.total_pages()
print("Num Pages: ", num_pages)

for i in range(num_pages):
    results = dataset.list_page(i)
