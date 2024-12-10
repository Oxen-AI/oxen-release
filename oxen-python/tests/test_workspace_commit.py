import os
from oxen import RemoteRepo, Workspace


def test_commit_one_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    # 1 commit pushed in setup
    assert len(remote_repo.log()) == 1
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    workspace = Workspace(remote_repo, "main")
    workspace.add(full_path)
    workspace.commit("a commit message!")
    assert len(remote_repo.log()) == 2


def test_commit_empty(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    workspace = Workspace(remote_repo, "main")
    workspace.commit("a commit message")
    assert len(remote_repo.log()) == 2
