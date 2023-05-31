import oxen
from oxen import Dataset
def test_image_classification_dataloader(
    celeba_remote_repo_fully_pushed, empty_local_dir
):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    local_repo, _remote_repo = celeba_remote_repo_fully_pushed

    train_file = "annotations/train.csv"

    dataset = Dataset(
        remote_repo,
        paths=[train_file],
        cache_dir=empty_local_dir
    )

    df = dataset.df(train_file)

    