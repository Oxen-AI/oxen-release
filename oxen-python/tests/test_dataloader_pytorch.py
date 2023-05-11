from torch.utils.data import DataLoader

from oxen import Dataset


def test_dataset_load_celeba_train_download(
    celeba_remote_repo_fully_pushed, empty_local_dir
):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    _local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # download the remote dataframe, and load the data into a dataloader
    cache_dir = empty_local_dir
    dataset = Dataset(remote_repo, cache_dir=cache_dir)
    train_files = ["annotations/train.csv", "images"]
    dataset.load(train_files, download=True)

    dataloader = DataLoader(dataset, batch_size=4, shuffle=False)

    for i, data in enumerate(dataloader, 0):
        # get the inputs; data is a list of [inputs, labels]
        inputs, labels = data
