from oxen import Repo, RemoteRepo


def test_repo_push(local_repo_one_image_committed: Repo, empty_remote_repo: RemoteRepo):
    local_repo = local_repo_one_image_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo_one_image_committed.push(remote_name, branch_name)
