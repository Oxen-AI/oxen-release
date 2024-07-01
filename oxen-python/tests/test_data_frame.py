import os

import pandas as pd

from oxen import DataFrame, RemoteRepo, Workspace

def test_data_frame_crud(
    celeba_remote_repo_fully_pushed
):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_row = {"file": "images/123456.png", "hair_color": "purple"}

    df = DataFrame(remote_repo.identifier, "annotations/train.csv", host="localhost:3000", scheme="http")
    _width, og_height = df.size()

    # List the rows
    rows = df.list_page(1)
    assert len(rows) > 0, "Error listing rows"

    # Add a row
    row_id = df.insert_row(new_row)
    _width, height = df.size()
    assert height == og_height + 1, "Error adding to test remove"

    # Update a row
    df.update_row(row_id, {"hair_color": "blue"})
    row = df.get_row_by_id(row_id)
    assert row["hair_color"] == "blue", "Error updating row"

    # Remove a row
    df.delete_row(row_id)
    _width, height = df.size()
    assert height == og_height, "Error removing row"


def test_data_frame_commit(
    celeba_remote_repo_fully_pushed
):
    _, remote_repo = celeba_remote_repo_fully_pushed

    new_row = {"file": "images/123456.png", "hair_color": "purple"}

    df = DataFrame(remote_repo.identifier, "annotations/train.csv", host="localhost:3000", scheme="http")

    # List commits before
    og_commits = remote_repo.log()

    # Add a row and commit
    df.insert_row(new_row)
    df.commit("Add row")
    
    # List commits after
    new_commits = remote_repo.log()
    assert len(new_commits) == len(og_commits) + 1, "Error committing row"



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
    df.delete_row(row_id)
    _width, height = df.size()
    assert height == og_height, "Error removing row"
