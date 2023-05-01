import os
from oxen import RemoteRepo

def test_remote_add_file(empty_remote_repo: RemoteRepo, shared_datadir):
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    empty_remote_repo.get_branch('main')
    empty_remote_repo.add(full_path, 'main', '')
    

