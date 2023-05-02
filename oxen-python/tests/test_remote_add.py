import os
from oxen import RemoteRepo

# def test_remote_add_file_new_branch(empty_remote_repo: RemoteRepo, shared_datadir):
#      full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
#      empty_remote_repo.get_branch('main')
#      empty_remote_repo.add(full_path, 'main', '')
    

def test_remote_add_file_existing_branch(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    remote_repo.add(full_path, 'main', '')
    


