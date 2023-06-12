# Data Loaders

Oxen provides a suite of pre-built data loaders for a variety of common machine learning tasks. These loaders make it easy to extract data from local or remote Oxen repositories and convert it into a format that's ready to use with your favorite machine learning framework.


## Using Loaders with Local Resositories 

For the below repo structure...

```
MyRepo
--- images/
------ image1.jpg 
------ ...
--- labels.txt
--- train.csv
--- test.csv 
```

...transform and load data with the following:

```python
import oxen 

repo = oxen.LocalRepo("MyRepo")

train_loader = oxen.loaders.ImageClassificationLoader(
    imagery_root_dir = f"{repo.path}",
    labels_file = f"{repo.path}/labels.txt", 
    df_file = f"{repo.path}/train.csv"
)

test_loader = oxen.loaders.ImageClassificationLoader(
    imagery_root_dir = f"{repo.path}",
    labels_file = f"{repo.path}/labels.txt", 
    df_file = f"{repo.path}/test.csv"
)

X_train, y_train, label_mapper = train_loader.run()
X_test, y_test, _ = test_loader.run()

# X_train: (50000 x 32 x 32 x 3)
# y_train: (50000,)
# mapper: {"cat": 0, "dog": 1, etc...}
```


## Defining Custom Loaders 

Oxen loaders are constructed as a Directed Acyclic Graph (DAG) of data operations. Each node in the graph inherits `oxen.Op` and defines a `call()` method. The `call()` method is responsible for executing the operation and returning the output.

These nodes are linked together to form a graph with specified inputs and outputs. The graph is then executed with a run() method, returning the outputs. 

### Example: Image Classification Loader


```python

class ImageClassificationLoader:
    def __init__(self, imagery_root_dir, label_file, df_file, path_name, label_name):
        # DEFINE INPUT NODES
        data_frame = ReadDF(input=df_file)
        label_list = ReadText(input=label_file) 
        path_name = Identity(input=path_name) 
        label_name = Identity(input=label_name)
        imagery_root_dir = Identity(input=imagery_root_dir) 

        # DEFINE INTERMEDIATE NODES
        paths = ExtractCol()(data_frame, path_name) 
        label_text = ExtractCol()(data_frame, label_name)

        # DEFINE OUTPUT NODES 
        images = ReadImageDir()(imagery_root_dir, paths) 
        label_map = CreateLabelMap()(label_list, label_text)
        labels = EncodeLabels()(label_text, label_map)

        # Create and compile the graph
        self.graph = DAG(outputs=[images, labels, label_map])
    
    def run():
        return self.graph.evaluate()

```

## Example loaders