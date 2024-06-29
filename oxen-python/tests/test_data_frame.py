import os

import pandas as pd

from oxen import DataFrame, RemoteRepo, Workspace


# def test_local_df_add_row_success(celeba_local_repo_no_commits):
#     repo = celeba_local_repo_no_commits

#     file_path = os.path.join(repo.path, "CelebA", "annotations", "train.csv")
#     df = DataFrame(repo.path, file_path)

#     new_row = {"file": "images/123456.png", "hair_color": "purple"}
#     df.insert_row(new_row)


def test_remove_data_frame_row(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed

    file_path = os.path.join(shared_datadir, "CelebA", "annotations", "train.csv")
    _df = pd.read_csv(file_path)
    workspace = Workspace(remote_repo, "main")
    print("Created workspace ", workspace)

    new_row = {"file": "images/123456.png", "hair_color": "purple"}

    assert len(workspace.status().added_files()) == 0
    workspace.add(file_path, "csvs")
    workspace.commit("add train.csv")

    workspace = Workspace(remote_repo, "main")
    df = DataFrame(workspace, "csvs/train.csv")
    _width, og_height = df.size()

    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == og_height + 1, "Error adding to test remove"
    df.remove_row(row_id)
    _width, height = df.size()
    assert height == og_height, "Error removing row"
