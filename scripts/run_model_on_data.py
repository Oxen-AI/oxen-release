from oxen import RemoteRepo
from oxen import RemoteDataset
import openai
import tqdm


print("Creating Remote Repo")
repo = RemoteRepo("ox/LLM-Dataset", "localhost:3001", scheme="http")

# Index the dataset
# from oxen.remote_dataset import index_dataset
# index_dataset(repo, "prompts.jsonl")

print("Creating Remote Dataset")
# Gets dataset if exists
dataset = RemoteDataset(repo, "prompts.parquet")

size = dataset.size()
print("size: ", size)

# Create openai client
client = openai.Client()
model = "gpt-4o"

results = dataset.list_page(1)
for result in tqdm.tqdm(results):
    print(result)
    prompt = result["input"]
    instruction = result["instruction"]

    completion = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt}
        ]
    )
    print("Assistant: " + completion.choices[0].message.content)
    response = completion.choices[0].message.content

    dataset.update_row(result["_oxen_id"], {"output": response})
