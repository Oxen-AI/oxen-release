import pytest
import logging

import uuid
import os

from oxen import Repo, RemoteRepo

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
    repo = Repo(empty_local_dir)
    repo.init()

    yield repo

@pytest.fixture
def celeba_local_repo(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
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
def local_repo_one_image_committed(celeba_local_repo):
    image_file = "images/1.jpg"
    full_path = os.path.join(celeba_local_repo.path, image_file)
    celeba_local_repo.add(full_path)
    celeba_local_repo.commit("Adding first image")
    yield celeba_local_repo

