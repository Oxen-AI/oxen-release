
import os

from oxen import Repo

def test_add(shared_datadir):
    repo_dir = os.path.join(shared_datadir, 'CelebA')
    repo = Repo(repo_dir)
    repo.init()
    image_file = os.path.join(repo_dir, 'images/1.jpg')
    repo.add(image_file)

