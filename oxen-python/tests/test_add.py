import os

from oxen import LocalRepo


def test_add(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = LocalRepo(repo_dir)
    repo.init()
    image_file = "annotations"
    full_path = os.path.join(repo_dir, image_file)
    repo.add(full_path)
    staged_data = repo.status()

    added_files = staged_data.added_files()
    added_files.sort()

    assert set(added_files) == {
        "annotations/test.csv",
        "annotations/train.csv",
        "annotations/labels.txt",
    }
