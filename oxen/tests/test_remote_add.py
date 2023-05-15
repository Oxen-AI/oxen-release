import os
from oxen import RemoteRepo


def test_remote_add_single_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed

    remote_repo.add(full_path, "a-folder")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()

    assert added_files == ["a-folder/1.jpg"]


def test_remote_add_root_dir(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    full_path = os.path.join(shared_datadir, "CelebA/images/3.jpg")

    _, remote_repo = celeba_remote_repo_one_image_pushed

    remote_repo.add(full_path, "")
    staged_data = remote_repo.status()
    added_files = staged_data.added_files()

    assert added_files == ["3.jpg"]
