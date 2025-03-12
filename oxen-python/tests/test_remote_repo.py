import os
from pathlib import PurePath

def test_remote_repo_exists(empty_remote_repo):
    exists = True
    assert empty_remote_repo.exists() == exists


def test_remote_repo_add(celeba_remote_repo_one_image_pushed, shared_datadir):
    _local_repo, remote_repo = celeba_remote_repo_one_image_pushed
    examples_path = str(PurePath("ChatBot", "examples.tsv"))
    full_path = os.path.join(shared_datadir, examples_path)
    remote_repo.add(full_path)
    status = remote_repo.status()
    added_files = status.added_files()
    assert len(added_files) == 1
    assert added_files[0] == "examples.tsv"
    commit = remote_repo.commit("Adding my image to the remote workspace.")
    print(commit)
    assert commit.id is not None
    assert commit.message == "Adding my image to the remote workspace."
