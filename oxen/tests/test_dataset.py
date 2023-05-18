from oxen import Dataset
import os


def test_dataset_load_celeba_train_download_df(
    celeba_remote_repo_fully_pushed, empty_local_dir
):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    _local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # download the remote dataframe, and load the data into a dataloader
    train_file = "annotations/train.csv"
    dataset = Dataset(
        remote_repo,
        paths=[train_file],
        cache_dir=empty_local_dir,
    )
    df = dataset.df(train_file)
    assert df is not None
    assert df.height == 5
    assert df.width == 2


def test_dataset_load_celeba_train_download_df_and_images(
    celeba_remote_repo_fully_pushed, empty_local_dir
):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    _local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # download the remote dataframe, and load the data into a dataloader
    train_file = "annotations/train.csv"
    images = "images"
    dataset = Dataset(
        remote_repo,
        paths=[train_file, images],
        cache_dir=empty_local_dir,
    )
    dataset.download_all()

    assert dataset.df(train_file) is not None
    images_dir = os.path.join(empty_local_dir, images)
    assert os.path.exists(images_dir)
    # 1.jpg, 2.jpg, ..., 9.jpg
    assert len(os.listdir(images_dir)) == 9
    for i in range(1, 10):
        assert os.path.exists(os.path.join(images_dir, f"{i}.jpg"))
