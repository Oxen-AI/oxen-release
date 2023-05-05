
def test_load_remote_dataframe(celeba_remote_repo_fully_pushed, empty_local_dir):
    # _local_repo is the original local repo
    # remote_repo is the remote repo we pushed to
    _local_repo, remote_repo = celeba_remote_repo_fully_pushed

    # download the remote dataframe, and load the data into a dataloader
    # dataloader = remote_repo.load()