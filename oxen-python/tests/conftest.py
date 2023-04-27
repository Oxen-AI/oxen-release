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


@pytest.fixture
def empty_remote_repo():
    repo_name = f"py-ox/test_repo_{str(uuid.uuid4())}"
    repo = RemoteRepo(repo_name, host=TEST_HOST)
    repo.create()
    yield repo
    repo.delete()


@pytest.fixture
def local_repo_one_image_committed(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()
    image_file = "images/1.jpg"
    full_path = os.path.join(repo_dir, image_file)
    repo.add(full_path)
    repo.commit("Adding first image")
    yield repo
