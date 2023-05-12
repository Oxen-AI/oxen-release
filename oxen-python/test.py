from oxen.dag import DAG

from oxen.ops import (
    Identity,
    ReadText,
    ReadDF,
    ExtractCol,
    StrColTemplate,
    ConcatSeries,
)

# import oxen

# repo = oxen.RemoteRepo("ox/CatsVsDogs", host="0.0.0.0:3001")

# train_file = "annotations/train.csv"
# dataset = oxen.Dataset(
#     repo,
#     paths=[train_file],
# )
# df = dataset.df(train_file)
# print(df)


# Define input nodes
prompt_name = Identity(input="prompt")
column_name = Identity(input="response")
prompt = ReadText(input="prompt.txt")
data_frame = ReadDF(input="data.csv")

# Define intermediate nodes
extract_prompt = ExtractCol(name="extract_prompt")(data_frame, prompt_name)
extract_response = ExtractCol(name="extract_response")(data_frame, column_name)
templatize = StrColTemplate(name="templatize")(prompt, extract_prompt)
output = ConcatSeries(name="concat_output")(templatize, extract_response)

# Create and compile the graph
graph = DAG(outputs=[output])

# Evaluate the graph with input data
result = graph.evaluate()

print("\n\nResult:")
print(result)
