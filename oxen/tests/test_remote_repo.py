def test_remote_repo_exists(empty_remote_repo):
    exists = True
    assert empty_remote_repo.exists() == exists
