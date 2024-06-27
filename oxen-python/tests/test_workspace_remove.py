import os
import pandas as pd
from oxen import RemoteRepo, Workspace, WorkspaceDataFrame


def test_remove_staged_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/2.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")

    workspace.add(full_path, "folder")
    status = workspace.status()
    added_files = status.added_files()
    assert len(added_files) == 1, "Error adding to test remove"

    workspace.rm("folder/2.jpg")
    status = workspace.status()
    added_files = status.added_files()
    assert len(added_files) == 0, "File not successfully removed from staging"


def test_restore_data_frame_row(
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
    remote_df = WorkspaceDataFrame(workspace, "csvs/train.csv")

    remote_df.insert_row(new_row)
    assert len(workspace.status().added_files()) == 1, "Error adding to test remove"
    remote_df.restore()
    assert len(workspace.status().added_files()) == 0, "DF row not removed"
