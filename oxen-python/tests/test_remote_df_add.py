# TODO: switch this to our own DF stuff later
import pandas as pd
import pytest
import os


def test_remote_df_add_row_success(celeba_remote_repo_one_image_pushed, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    df = pd.read_csv(file_path)

    new_row = {"file": "images/123456.png", "hair_color": "purple"}

    remote_repo.add(file_path, "csvs")
    remote_repo.commit("add train.csv")
    remote_repo.add_df_row(path="csvs/train.csv", row=new_row)
    remote_repo.commit("add row to train.csv")

    # Download the file
    remote_repo.download("csvs/train.csv", file_path)

    # Check the new file
    new_df = pd.read_csv(file_path)

    # Row added:
    assert len(new_df) == len(df) + 1
    # Check row values:
    assert new_df.iloc[-1].file == "images/123456.png"
    assert new_df.iloc[-1].hair_color == "purple"


def test_remote_df_add_row_invalid_schema(
    celeba_remote_repo_one_image_pushed, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    # df = pd.read_csv(file_path)

    new_row = {"gahfile": "images/123456.png", "hair_color": "purple"}

    remote_repo.add(file_path, "csvs")
    remote_repo.commit("add train.csv")
    with pytest.raises(ValueError):
        remote_repo.add_df_row(path="csvs/train.csv", row=new_row)
