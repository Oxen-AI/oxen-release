from oxen.dag import DAG

from oxen.ops import Identity, ReadDF, ExtractCol, ConcatSeries


class RegressionLoader:
    def __init__(self, data_file, pred_name, f_names):
        # Define input nodes
        pred_name = Identity(input=pred_name)
        data_frame = ReadDF(input=data_file)

        # Define intermediate nodes
        prediction = ExtractCol()(data_frame, pred_name)
        extracts = [ExtractCol()(data_frame, Identity(input=col)) for col in f_names]
        features = ConcatSeries(name="concat")(*extracts)

        # Create and compile the graph
        self.graph = DAG(outputs=[prediction, features])

    def run(self):
        # Run the graph to get the outputs
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
