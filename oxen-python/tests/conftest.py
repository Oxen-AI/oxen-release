import pytest
import logging

import uuid
import os

from oxen import LocalRepo, RemoteRepo

FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT)

if "OXEN_TEST_LOG" in os.environ:
    level = os.environ["OXEN_TEST_LOG"].lower()
    if "debug" == level:
        logging.getLogger().setLevel(logging.DEBUG)
    elif "info" == level:
        logging.getLogger().setLevel(logging.INFO)

TEST_HOST = "0.0.0.0:3000"
if "OXEN_TEST_HOST" in os.environ:
    TEST_HOST = os.environ["OXEN_TEST_HOST"]

# These fixtures build on each other to represent different states
# of oxen repos. For example we have:
# * empty local dir
# * empty local repo
# * empty remote repo
# * local repos with data committed
# * remote repos with data pushed
# * local repos with data committed
# * remote repos with data pushed

# shared_data_dir is a pytest fixture that points to the shared data
# that we define in tests/data and takes setup and cleaning up after


@pytest.fixture
def empty_local_dir(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "empty_repo")
    repo_name = f"test_repo_{str(uuid.uuid4())}"
    yield os.path.join(repo_dir, repo_name)


@pytest.fixture
def empty_local_repo(empty_local_dir):
    repo = LocalRepo(empty_local_dir)
    repo.init()

    yield repo


@pytest.fixture
def celeba_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = LocalRepo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def chat_bot_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "ChatBot")
    repo = LocalRepo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def house_prices_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "HousePrices")
    repo = LocalRepo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def empty_remote_repo():
    repo_name = f"py-ox/test_repo_{str(uuid.uuid4())}"
    repo = RemoteRepo(repo_name, host=TEST_HOST)
    repo.create()
    yield repo
    repo.delete()


@pytest.fixture
def celeba_local_repo_one_image_committed(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    image_file = "images/1.jpg"
    full_path = os.path.join(repo.path, image_file)
    repo.add(full_path)
    repo.commit("Adding first image")
    yield repo


@pytest.fixture
def celeba_local_repo_fully_committed(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    repo.add(os.path.join(repo.path, "images"))
    repo.add(os.path.join(repo.path, "annotations"))
    repo.commit("Adding all data")
    yield repo


@pytest.fixture
def celeba_remote_repo_one_image_pushed(
    celeba_local_repo_one_image_committed, empty_remote_repo
):
    local_repo = celeba_local_repo_one_image_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


@pytest.fixture
def celeba_remote_repo_fully_pushed(
    celeba_local_repo_fully_committed, empty_remote_repo
):
    local_repo = celeba_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo
