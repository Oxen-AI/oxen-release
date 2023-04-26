import os

from oxen import Repo


def test_init(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "empty_repo")
    repo = Repo(repo_dir)
    repo.init()

    assert os.path.exists(os.path.join(repo_dir, ".oxen"))
