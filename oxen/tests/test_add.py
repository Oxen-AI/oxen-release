import os

from oxen import Repo


def test_add(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()
    image_file = "images/1.jpg"
    full_path = os.path.join(repo_dir, image_file)
    repo.add(full_path)
    staged_data = repo.status()
    assert staged_data.added_files() == [image_file]
