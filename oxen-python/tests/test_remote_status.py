import os
from oxen import RemoteRepo


def test_remote_status_empty(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    remote_repo.add(full_path, 'main', '')
    staged_data = remote_repo.status("main", "")
    assert len(staged_data.added_files()) == 1
    
