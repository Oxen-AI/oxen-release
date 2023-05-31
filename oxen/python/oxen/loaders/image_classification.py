from oxen.dag import DAG

from oxen.ops import (
    Identity,
    ReadText,
    ReadDF,
    ExtractCol,
    StrColTemplate,
    ConcatSeries,
    ReadImageDir,
)

# Image Classification Loader Graph

# csv_input = oxen.DataFrameLoader("annotations/train.csv")
# images_input = oxen.DirLoader("images")
# labels_input = oxen.FileLoader("labels.txt")

# image_column = oxen.ColumnExtractor(["image"])(csv_input)
# label_column = oxen.ColumnExtractor(["label"])(csv_input)

# line_to_idx = oxen.LineToIdx()(label_column, labels_input)

# image_output = oxen.ImageTensor()(image_column, images_input)
# label_output = oxen.LabelTensor()(label_column, line_to_idx)

# lag = oxen.LoaderGraph(
#     inputs=[csv_input, images_input, labels_input],
#     outputs=[image_output, label_output],
# )


class ImageClassificationLoader:
    def __init__(self, images_dir, csv_file, path_name, label_name):
        # Define input nodes 
        data_frame = ReadDf(input = csv_file)
        path_name = Identity(input=pred_name) 
        label_name = Identity(input=label_name)
        images_dir = Identity(input=images_dir)

        # Define intermediate nodes 
            # Extract relevant columns
        paths = ExtractCol()(data_frame, path_name)
        labels = ExtractCol()(data_frame, label_name)
            # Read in imagery 
        images = ReadImageDir()(images_dir, paths)

        # Load Labels File (HOW? / TODO)

        # Extract Image and Label columns from DF 

        # Convert Label column to indices, save 

        # Convert image colu
        

    def run(self):
        # Run the graph to get the outputs
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
