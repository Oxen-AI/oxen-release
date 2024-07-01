import pandas as pd
import pytest
import os
from oxen import Workspace, DataFrame


def test_workspace_df_add_row_success(
    celeba_remote_repo_one_image_pushed, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    df = pd.read_csv(file_path)
    workspace.add(file_path, "csvs")
    workspace.commit("add train.csv")

    new_row = {"file": "images/123456.png", "hair_color": "purple"}
    remote_df = DataFrame(workspace, "csvs/train.csv")
    remote_df.insert_row(new_row)
    workspace.commit("add row to train.csv")

    # Download the file
    remote_repo.download("csvs/train.csv", file_path)

    # Check the new file
    new_df = pd.read_csv(file_path)

    # Row added:
    assert len(new_df) == len(df) + 1
    # Check row values:
    assert new_df.iloc[-1].file == new_row["file"]
    assert new_df.iloc[-1].hair_color == new_row["hair_color"]


def test_remote_df_add_row_invalid_schema(
    celeba_remote_repo_one_image_pushed, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    # df = pd.read_csv(file_path)

    new_row = {"gahfile": "images/123456.png", "hair_color": "purple"}

    workspace.add(file_path, "csvs")
    workspace.commit("add train.csv")
    remote_df = DataFrame(workspace, "csvs/train.csv")
    with pytest.raises(ValueError):
        remote_df.insert_row(new_row)
