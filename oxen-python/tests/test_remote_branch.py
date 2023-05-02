import os
from oxen import RemoteRepo

def test_create_get_new_branch(empty_remote_repo: RemoteRepo, shared_datadir):
    empty_remote_repo.get_branch('main')

def test_create_get_existing_branch(celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    remote_repo.create_or_get_branch('hrllo')
    remote_repo.get_branch('hrllo')


