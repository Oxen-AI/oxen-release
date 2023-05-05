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
    train_file = "annotations/train.csv"
    dataset.load(train_file, download=True)
    df = dataset.df(train_file)
    assert df is not None
    assert df.height == 5
    assert df.width == 2
