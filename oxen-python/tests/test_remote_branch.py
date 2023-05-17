from oxen import RemoteRepo


def test_create_new_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("hrllo")
    remote_repo.checkout("hrllo")

    assert len(remote_repo.list_branches()) == 2


def test_create_existing_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("main")

    assert len(remote_repo.list_branches()) == 1
