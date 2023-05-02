import os
from oxen import RemoteRepo

def test_commit_one_file(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    remote_repo.add(full_path, 'main', '')
    remote_repo.commit("a commit message!", "main")
    remote_repo.log("main")