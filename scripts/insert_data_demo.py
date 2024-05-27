from oxen import RemoteRepo
from oxen import RemoteDataset

print("Creating Remote Repo")
repo = RemoteRepo("ox/Branchy-Branch", "localhost:3001", scheme="http")

print("Creating Remote Dataset")
# Gets dataset if exists
dataset = RemoteDataset(repo, "questions.jsonl")

size = dataset.size()
print("size: ", size)

results = dataset.list()
for result in results:
    print(result)

print("Inserting One Row")
id = dataset.insert_one({"prompt": "can I get from python?", "response": "yes you can!"})
print(id)

# id = "2ea5f604-be1a-4304-87b7-4c50f7f51a0c"
print("Get that row back")
row = dataset.get_by_id(id)