import os
from oxen import RemoteRepo


def test_remove_staged_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/2.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed

    remote_repo.add(full_path, "folder")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()
    assert len(added_files) == 1, "Error adding to test remove"

    remote_repo.remove("folder/2.jpg")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()
    assert len(added_files) == 0, "File not successfully removed from staging"
