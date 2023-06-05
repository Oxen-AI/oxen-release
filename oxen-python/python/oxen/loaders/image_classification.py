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
    def __init__(self, imagery_root_dir, label_file, csv_file, path_name, label_name):
        # Define input nodes
        data_frame = ReadDF(input=csv_file)
        label_list = ReadText(input=label_file)
        path_name = Identity(input=path_name)
        label_name = Identity(input=label_name)
        imagery_root_dir = Identity(input=imagery_root_dir)

        # Define intermediate nodes
        paths = ExtractCol()(data_frame, path_name)
        label_text = ExtractCol()(data_frame, label_name)

        images = ReadImageDir()(imagery_root_dir, paths)
        label_map = CreateLabelMap()(label_list, label_text)
        labels = EncodeLabels()(label_text, label_map)

        # Create and compile the graph
        self.graph = DAG(outputs=[images, labels, label_map])

    def run(self):
        # Run the graph to get the outputs
        result = self.graph.evaluate()

        print("\n\nResult:")
        print(result)
        return result
