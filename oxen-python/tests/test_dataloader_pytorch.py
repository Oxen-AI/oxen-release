# from torch.utils.data import DataLoader

# from oxen import Dataset, DataEntry, Features


# def test_dataset_load_celeba_train_download(
#     celeba_remote_repo_fully_pushed, empty_local_dir
# ):
# _local_repo is the original local repo
# remote_repo is the remote repo we pushed to
# _local_repo, remote_repo = celeba_remote_repo_fully_pushed

# download the remote dataframe, and load the data into a dataloader
# cache_dir = empty_local_dir

#  "images",   "annotations/train.csv"   "labels.txt"
#         |                  /                |
#         |                 /                 |
#

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

# LLM Loader Graph

# csv_input = oxen.DataFrameLoader("train.csv")
# prompt_input = oxen.FileLoader("prompt.txt")

# prompt_column = oxen.ColumnExtractor(["prompt"])(csv_input)
# response_column = oxen.ColumnExtractor(["response"])(csv_input)

# prompt_column = oxen.PromptTemplate()(prompt_input, prompt_column)

# prompt_output = oxen.TextTokenizer()(prompt_column)
# response_output = oxen.TextTokenizer()(response_column)

# lag = oxen.LoaderGraph(
#     inputs=[csv_input, prompt_input],
#     outputs=[prompt_output, response_output],
# )

# House Price Regression

# csv_input = oxen.DataFrameLoader("housing.csv")
# price = oxen.ColumnExtractor(["price"])(csv_input)
# features = oxen.ColumnExtractor(["sqft", "num_bed", "num_bath"])(csv_input)

# lag = oxen.LoaderGraph(
#     inputs=[csv_input],
#     outputs=[price, features],
# )

# dataset = Dataset(remote_repo, lag, cache_dir=cache_dir, download=True)

# # train_files = ["annotations/train.csv", "images"]
# # dataset.load(train_files, download=True)

# dataloader = DataLoader(dataset, batch_size=4, shuffle=False)

# for i, data in enumerate(dataloader, 0):
#     # get the inputs; data is a list of [inputs, labels]
#     inputs, labels = data
