import os
from oxen import RemoteRepo
from oxen import Workspace


def test_workspace_add_single_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    workspace.add(full_path, "a-folder")
    status = workspace.status()
    added_files = status.added_files()

    assert added_files == ["a-folder/1.jpg"]


def test_workspace_add_root_dir(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/3.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main", "test-workspace")

    workspace.add(full_path, "")
    status = workspace.status()
    added_files = status.added_files()

    assert added_files == ["3.jpg"]
