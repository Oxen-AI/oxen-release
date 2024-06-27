import os
from oxen import RemoteRepo, Workspace


def test_remote_status_empty(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")
    status = workspace.status()
    assert len(status.added_files()) == 0


def test_remote_status_after_add(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    workspace = Workspace(remote_repo, "main")
    workspace.add(full_path)
    status = workspace.status()
    assert status.added_files() == ["1.jpg"]
