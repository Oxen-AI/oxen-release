from oxen.dag import DAG

from oxen.ops import Identity, ReadDF, ExtractCol, ConcatSeries


class RegressionLoader:
    def __init__(self, data_file, pred_name, f_names):
        """
        Extracts and formats relevant features and labels
        from a tabular dataset for use in regression tasks.

        Parameters
        ----------
        data_file : str
            Path to a tabular file containing the input features
            and prediction target for a regression task
        pred_nam : str
            Column name in data_file containing the prediction target
        f_names : list
            List of column names in data_file containing the input features
        """
        # Define input nodes
        pred_name = Identity(input=pred_name)
        data_frame = ReadDF(input=data_file)

        # Define intermediate nodes
        prediction = ExtractCol()(data_frame, pred_name)
        extracts = [ExtractCol()(data_frame, Identity(input=col)) for col in f_names]
        features = ConcatSeries(name="concat")(*extracts)

        # Create and compile the graph
        self.graph = DAG(outputs=[features, prediction])

    def run(self):
        # Run the graph to get the outputs
        """
        Returns
        ---------
        outputs[0] (features) : pl.DataFrame
            DataFrame containing only the specified input features
        outputs[1] (prediction) : pl.Series
            Series containing the prediction target
        """
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
