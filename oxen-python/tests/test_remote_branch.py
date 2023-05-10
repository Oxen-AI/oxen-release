from oxen import RemoteRepo


def test_create_existing_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_branch("hrllo")
    remote_repo.get_branch("hrllo")
    # TODO: Getters for branch name?


def test_get_existing_branch(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.get_branch("main")
    # TODO: Getters for branch name?
