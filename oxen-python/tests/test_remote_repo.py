from oxen import RemoteRepo
import tests
import pytest
import uuid


@pytest.fixture
def remote_repo():
    repo_name = f"py-ox/test_repo_{str(uuid.uuid4())}"
    repo = RemoteRepo(repo_name, host=tests.host)
    repo.create()
    yield repo
    repo.delete()


def test_create_remote(remote_repo):
    exists = True
    assert remote_repo.exists() == exists
