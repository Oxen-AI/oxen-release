from oxen.dag import DAG

from oxen.ops import (
    Identity,
    ReadDF,
    ExtractCol,
    CreateLabelMap,
    EncodeLabels,
    ReadImageDir,
    ReadText,
)


class ImageClassificationLoader:
    """
    Prepares data from an Oxen repository for use in supervised image classification tasks.
    """
    def __init__(self, imagery_root_dir, label_file, df_file, path_name="path", label_name="label"):
        """
        Creates a new ImageClassificationLoader.

        Parameters
        ----------
        imagery_root_dir : str
            Directory relative to which the image paths in the CSV file are specified. 
        label_file: str
            Path to a text file containing a line-separated list of canonical labels for the dataset.
        df_file : str
            Path to a tabular file containing the image paths and associate labels (and any additional metadata).
        path_name : str 
            Column name in df_file containing the image paths 
        label_name : str
            Column name in df_file containing the image labels
        """
        # Define input nodes
        data_frame = ReadDF(input=df_file)
        label_list = ReadText(input=label_file)
        path_name = Identity(input=path_name)
        label_name = Identity(input=label_name)
        imagery_root_dir = Identity(input=imagery_root_dir)

        # Define intermediate nodes
        paths = ExtractCol()(data_frame, path_name)
        label_text = ExtractCol()(data_frame, label_name)

        # Define output nodes
        images = ReadImageDir()(imagery_root_dir, paths)
        label_map = CreateLabelMap()(label_list, label_text)
        labels = EncodeLabels()(label_text, label_map)

        # Create and compile the graph
        self.graph = DAG(outputs=[images, labels, label_map])

    def run(self):
        """
        Returns 
        -------
        outputs[0] (images) : np.ndarray
            All images found in the dataset, as a numpy array of shape (n, h, w, c)
        outputs[1] (labels) : np.nadarray
            Encoded labels for training, index-matched to the images array 
        outputs[2] (mapper) : dict
            A dictionary mapping the encoded labels to their canonical names
        """
        # Run the graph to get the outputs
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
