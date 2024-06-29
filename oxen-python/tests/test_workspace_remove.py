import os
from oxen import RemoteRepo, Workspace


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
