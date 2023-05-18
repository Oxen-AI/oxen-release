import os

from oxen import LocalRepo


def test_status_empty(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = LocalRepo(repo_dir)
    repo.init()
    staged_data = repo.status()
    assert len(staged_data.added_files()) == 0
