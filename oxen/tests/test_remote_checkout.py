import os


def test_checkout_then_add_new_branch(
    celeba_remote_repo_one_image_pushed, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("newbranch")
    remote_repo.checkout("newbranch")

    full_path = os.path.join(shared_datadir, "CelebA/images/1.jpg")
    remote_repo.add(full_path)
    staged_data = remote_repo.status()

    staged_data_main = remote_repo.status("main")

    assert len(staged_data_main.added_files()) == 0
    assert len(staged_data.added_files()) == 1
