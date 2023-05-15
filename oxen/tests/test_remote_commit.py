import os
from oxen import RemoteRepo


def test_commit_one_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    # Initial commit, +1 pushed in setup
    assert len(remote_repo.log()) == 2
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    remote_repo.add(full_path)
    remote_repo.commit("a commit message!")
    assert len(remote_repo.log()) == 3


def test_commit_empty(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.commit("a commit message")
    assert len(remote_repo.log()) == 3
