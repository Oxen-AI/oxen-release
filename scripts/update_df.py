from oxen import RemoteRepo
from oxen import RemoteDataset
from oxen.remote_dataset import index_dataset
import time

print("Creating Remote Repo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")
print("Repo object created")

file_name = "prompts.parquet"

# Loop over 10 questions
questions = [
    {"input": "What is the capital of France?", "instruction": "Answer the question plz.", "output": "Paris"},
    {"input": "What is the capital of Germany?", "instruction": "Answer the question plz.", "output": "Berlin"},
    {"input": "What is the capital of Italy?", "instruction": "Answer the question plz.", "output": "Rome"},
    {"input": "What is the capital of Spain?", "instruction": "Answer the question plz.", "output": "Madrid"},
    {"input": "What is the capital of Portugal?", "instruction": "Answer the question plz.", "output": "Lisboa"},
    {"input": "What is the capital of Switzerland?", "instruction": "Answer the question plz.", "output": "Bern"},
    {"input": "What is the capital of Austria?", "instruction": "Answer the question plz.", "output": "Vienna"},
    {"input": "What is the capital of Poland?", "instruction": "Answer the question plz.", "output": "Warsaw"},
    {"input": "What is the capital of Turkey?", "instruction": "Answer the question plz.", "output": "Ankara"},
    {"input": "What is the capital of Belgium?", "instruction": "Answer the question plz.", "output": "Brussels"},
    {"input": "What is the capital of Netherlands?", "instruction": "Answer the question plz.", "output": "Amsterdam"},
    {"input": "What is the capital of Luxembourg?", "instruction": "Answer the question plz.", "output": "Luxembourg"},
    {"input": "What is the capital of Ireland?", "instruction": "Answer the question plz.", "output": "Dublin"},
]

index_times = []
connection_times = []
insert_times = []
commit_times = []

for i, question in enumerate(questions):
    # time indexing
    print("==== Indexing dataset", i, "====")
    start = time.time()
    index_dataset(repo, file_name)
    end = time.time()
    index_time = end - start
    print("Indexing time: ", index_time)
    index_times.append(index_time)

    start = time.time()
    print("Connecting to Remote Dataset")
    # Gets dataset if exists
    dataset = RemoteDataset(repo, file_name)
    connection_time = time.time() - start
    print("Connection time: ", connection_time)
    connection_times.append(connection_time)

    start = time.time()
    id = dataset.insert_row(question)
    print("Inserted row with id: ", id)
    insert_time = time.time() - start
    print("Insert time: ", insert_time)
    insert_times.append(insert_time)

    start = time.time()
    commit_id = dataset.commit("Added question: " + question["input"])
    print("Committed row with id: ", commit_id)
    commit_time = time.time() - start
    print("Commit time: ", commit_time)
    commit_times.append(commit_time)
    break

# print average times
print("Average indexing time: ", sum(index_times) / len(index_times))
print("Average connection time: ", sum(connection_times) / len(connection_times))
print("Average insert time: ", sum(insert_times) / len(insert_times))
print("Average commit time: ", sum(commit_times) / len(commit_times))
