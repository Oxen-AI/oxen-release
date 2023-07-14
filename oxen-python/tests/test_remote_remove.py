import os
import pandas as pd
from oxen import RemoteRepo


def test_remove_staged_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/2.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed

    remote_repo.add(full_path, "folder")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()
    assert len(added_files) == 1, "Error adding to test remove"

    remote_repo.remove("folder/2.jpg")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()
    assert len(added_files) == 0, "File not successfully removed from staging"


def test_restore_staged_df_row(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    _df = pd.read_csv(file_path)

    new_row = {"file": "images/123456.png", "hair_color": "purple"}

    assert len(remote_repo.status().added_files()) == 0
    remote_repo.add(file_path, "csvs")
    remote_repo.commit("add train.csv")
    remote_repo.add_df_row(path="csvs/train.csv", row=new_row)
    assert len(remote_repo.status().added_files()) == 1, "Error adding to test remove"
    remote_repo.restore_df(path="csvs/train.csv")
    assert len(remote_repo.status().added_files()) == 0, "DF row not removed"
