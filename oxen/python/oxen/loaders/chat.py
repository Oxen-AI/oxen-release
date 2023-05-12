from oxen.dag import DAG

from oxen.ops import (
    Identity,
    ReadText,
    ReadDF,
    ExtractCol,
    StrColTemplate,
    ConcatSeries,
)


class ChatLoader:
    def __init__(self, prompt_file, data_file):
        # Define input nodes
        prompt_name = Identity(input="prompt")
        column_name = Identity(input="response")
        prompt = ReadText(input=prompt_file)
        data_frame = ReadDF(input=data_file)

        # Define intermediate nodes
        extract_prompt = ExtractCol(name="extract_prompt")(data_frame, prompt_name)
        extract_response = ExtractCol(name="extract_response")(data_frame, column_name)
        templatize = StrColTemplate(name="templatize")(prompt, extract_prompt)
        output = ConcatSeries(name="concat_output")(templatize, extract_response)

        # Create and compile the graph
        self.graph = DAG(outputs=[output])

    def run(self):
        # Run the graph to get the outputs
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
