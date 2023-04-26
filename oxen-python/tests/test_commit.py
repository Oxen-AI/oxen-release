import os

from oxen import Repo


def test_commit(shared_datadir):
    # Use the shared_datadir fixture to get the path to the CelebA dataset
    repo_dir = os.path.join(shared_datadir, "CelebA")

    # oxen init
    repo = Repo(repo_dir)
    repo.init()

    # oxen add
    image_file = "images/1.jpg"
    full_path = os.path.join(repo_dir, image_file)
    repo.add(full_path)

    # oxen commit
    repo.commit("Add first image")

    # oxen log
    history = repo.log()
    # There is always an initial commit + the one we just made
    assert len(history) == 2
