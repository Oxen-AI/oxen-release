import os
from oxen import Workspace


def test_workspace_add_to_branch(celeba_remote_repo_one_image_pushed, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("newbranch")
    remote_repo.checkout("newbranch")

    workspace = Workspace(remote_repo, "newbranch")

    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    workspace.add(full_path)
    status = workspace.status()

    assert len(status.added_files()) == 1
