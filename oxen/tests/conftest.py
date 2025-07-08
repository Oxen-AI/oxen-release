import pytest
import logging
import tempfile
import shutil
import json
from unittest.mock import Mock, MagicMock

import uuid
import os
from pathlib import PurePath, Path

from oxen import Repo, RemoteRepo

FORMAT = "%(levelname)s %(name)s %(asctime)-15s %(filename)s:%(lineno)d %(message)s"
logging.basicConfig(format=FORMAT)

if "OXEN_TEST_LOG" in os.environ:
    level = os.environ["OXEN_TEST_LOG"].lower()
    if "debug" == level:
        logging.getLogger().setLevel(logging.DEBUG)
    elif "info" == level:
        logging.getLogger().setLevel(logging.INFO)

TEST_SCHEME = "http"
TEST_HOST = "localhost:3000"
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
def celeba_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "CelebA")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def question_embeddings_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "QuestionEmbeddings")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def question_embeddings_local_repo_fully_committed(
    question_embeddings_local_repo_no_commits,
):
    repo = question_embeddings_local_repo_no_commits

    repo.add(os.path.join(repo.path, "chunk_embeddings.parquet"))
    repo.add(os.path.join(repo.path, "question_embeddings.parquet"))
    repo.add(os.path.join(repo.path, "smol.jsonl"))
    repo.commit("Adding question and chunk embeddings")
    yield repo


@pytest.fixture
def question_embeddings_remote_repo_fully_pushed(
    question_embeddings_local_repo_fully_committed, empty_remote_repo
):
    local_repo = question_embeddings_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


# Additional common fixtures for testing infrastructure

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Provide a mock configuration object for testing."""
    config = MagicMock()
    config.get = Mock(side_effect=lambda key, default=None: {
        "api_key": "test_api_key_123",
        "host": "localhost:3000",
        "scheme": "http",
        "timeout": 30,
        "max_retries": 3
    }.get(key, default))
    return config


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.3, 78.9, 95.1, 88.7],
        'active': [True, False, True, True, False]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "version": "1.0.0",
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "author": "test_user"
        },
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response for testing API calls."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    content = """id,name,value
1,Item1,100
2,Item2,200
3,Item3,300"""
    with open(csv_path, 'w') as f:
        f.write(content)
    yield csv_path


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    import pandas as pd
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    df = pd.DataFrame({
        'id': range(100),
        'value': [f'value_{i}' for i in range(100)]
    })
    df.to_parquet(parquet_path)
    yield parquet_path


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up any test files created during tests."""
    created_files = []
    
    def register_file(filepath):
        created_files.append(filepath)
    
    request.addfinalizer(lambda: [
        os.remove(f) for f in created_files if os.path.exists(f)
    ])
    
    return register_file


@pytest.fixture
def mock_remote_repo():
    """Create a mock RemoteRepo for testing without actual network calls."""
    repo = Mock(spec=RemoteRepo)
    repo.identifier = "test-user/test-repo"
    repo.host = "localhost:3000"
    repo.scheme = "http"
    repo.url = "http://localhost:3000/test-user/test-repo"
    repo.exists.return_value = True
    return repo


@pytest.fixture
def parquet_files_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "parquet")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def parquet_files_local_repo_fully_committed(parquet_files_local_repo_no_commits):
    repo = parquet_files_local_repo_no_commits

    repo.add(repo.path)
    repo.commit("Adding parquet files")
    yield repo


@pytest.fixture
def parquet_files_remote_repo_fully_pushed(
    parquet_files_local_repo_fully_committed, empty_remote_repo
):
    local_repo = parquet_files_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


# Additional common fixtures for testing infrastructure

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Provide a mock configuration object for testing."""
    config = MagicMock()
    config.get = Mock(side_effect=lambda key, default=None: {
        "api_key": "test_api_key_123",
        "host": "localhost:3000",
        "scheme": "http",
        "timeout": 30,
        "max_retries": 3
    }.get(key, default))
    return config


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.3, 78.9, 95.1, 88.7],
        'active': [True, False, True, True, False]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "version": "1.0.0",
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "author": "test_user"
        },
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response for testing API calls."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    content = """id,name,value
1,Item1,100
2,Item2,200
3,Item3,300"""
    with open(csv_path, 'w') as f:
        f.write(content)
    yield csv_path


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    import pandas as pd
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    df = pd.DataFrame({
        'id': range(100),
        'value': [f'value_{i}' for i in range(100)]
    })
    df.to_parquet(parquet_path)
    yield parquet_path


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up any test files created during tests."""
    created_files = []
    
    def register_file(filepath):
        created_files.append(filepath)
    
    request.addfinalizer(lambda: [
        os.remove(f) for f in created_files if os.path.exists(f)
    ])
    
    return register_file


@pytest.fixture
def mock_remote_repo():
    """Create a mock RemoteRepo for testing without actual network calls."""
    repo = Mock(spec=RemoteRepo)
    repo.identifier = "test-user/test-repo"
    repo.host = "localhost:3000"
    repo.scheme = "http"
    repo.url = "http://localhost:3000/test-user/test-repo"
    repo.exists.return_value = True
    return repo


@pytest.fixture
def chat_bot_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "ChatBot")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def chat_bot_local_repo_fully_committed(chat_bot_local_repo_no_commits):
    repo = chat_bot_local_repo_no_commits

    repo.add(os.path.join(repo.path, "examples.tsv"))
    repo.add(os.path.join(repo.path, "prompt.txt"))
    repo.add(os.path.join(repo.path, "formats"))
    repo.commit("Adding all data")
    yield repo


@pytest.fixture
def chat_bot_remote_repo_fully_pushed(
    chat_bot_local_repo_fully_committed, empty_remote_repo
):
    local_repo = chat_bot_local_repo_fully_committed
    remote_repo = empty_remote_repo

    remote_name = "origin"
    branch_name = "main"
    local_repo.set_remote(remote_name, remote_repo.url)
    local_repo.push(remote_name, branch_name)

    yield local_repo, remote_repo


# Additional common fixtures for testing infrastructure

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Provide a mock configuration object for testing."""
    config = MagicMock()
    config.get = Mock(side_effect=lambda key, default=None: {
        "api_key": "test_api_key_123",
        "host": "localhost:3000",
        "scheme": "http",
        "timeout": 30,
        "max_retries": 3
    }.get(key, default))
    return config


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.3, 78.9, 95.1, 88.7],
        'active': [True, False, True, True, False]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "version": "1.0.0",
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "author": "test_user"
        },
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response for testing API calls."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    content = """id,name,value
1,Item1,100
2,Item2,200
3,Item3,300"""
    with open(csv_path, 'w') as f:
        f.write(content)
    yield csv_path


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    import pandas as pd
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    df = pd.DataFrame({
        'id': range(100),
        'value': [f'value_{i}' for i in range(100)]
    })
    df.to_parquet(parquet_path)
    yield parquet_path


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up any test files created during tests."""
    created_files = []
    
    def register_file(filepath):
        created_files.append(filepath)
    
    request.addfinalizer(lambda: [
        os.remove(f) for f in created_files if os.path.exists(f)
    ])
    
    return register_file


@pytest.fixture
def mock_remote_repo():
    """Create a mock RemoteRepo for testing without actual network calls."""
    repo = Mock(spec=RemoteRepo)
    repo.identifier = "test-user/test-repo"
    repo.host = "localhost:3000"
    repo.scheme = "http"
    repo.url = "http://localhost:3000/test-user/test-repo"
    repo.exists.return_value = True
    return repo


@pytest.fixture
def house_prices_local_repo_no_commits(shared_datadir):
    repo_dir = os.path.join(shared_datadir, "HousePrices")
    repo = Repo(repo_dir)
    repo.init()

    yield repo


@pytest.fixture
def house_prices_local_repo_fully_committed(house_prices_local_repo_no_commits):
    repo = house_prices_local_repo_no_commits

    repo.add(os.path.join(repo.path, "prices.csv"))
    repo.commit("Add prices.csv")

    yield repo


@pytest.fixture
def empty_remote_repo():
    repo_name = f"py-ox/test_repo_{str(uuid.uuid4())}"
    repo = RemoteRepo(repo_name, host=TEST_HOST, scheme=TEST_SCHEME)
    repo.create(empty=True)
    yield repo
    repo.delete()


@pytest.fixture
def celeba_local_repo_one_image_committed(celeba_local_repo_no_commits):
    repo = celeba_local_repo_no_commits

    image_file = str(PurePath("images", "1.jpg"))
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

    remote_repo = RemoteRepo(remote_repo.identifier, host=TEST_HOST, scheme=TEST_SCHEME)

    yield local_repo, remote_repo


# Additional common fixtures for testing infrastructure

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Provide a mock configuration object for testing."""
    config = MagicMock()
    config.get = Mock(side_effect=lambda key, default=None: {
        "api_key": "test_api_key_123",
        "host": "localhost:3000",
        "scheme": "http",
        "timeout": 30,
        "max_retries": 3
    }.get(key, default))
    return config


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.3, 78.9, 95.1, 88.7],
        'active': [True, False, True, True, False]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "version": "1.0.0",
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "author": "test_user"
        },
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response for testing API calls."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    content = """id,name,value
1,Item1,100
2,Item2,200
3,Item3,300"""
    with open(csv_path, 'w') as f:
        f.write(content)
    yield csv_path


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    import pandas as pd
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    df = pd.DataFrame({
        'id': range(100),
        'value': [f'value_{i}' for i in range(100)]
    })
    df.to_parquet(parquet_path)
    yield parquet_path


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up any test files created during tests."""
    created_files = []
    
    def register_file(filepath):
        created_files.append(filepath)
    
    request.addfinalizer(lambda: [
        os.remove(f) for f in created_files if os.path.exists(f)
    ])
    
    return register_file


@pytest.fixture
def mock_remote_repo():
    """Create a mock RemoteRepo for testing without actual network calls."""
    repo = Mock(spec=RemoteRepo)
    repo.identifier = "test-user/test-repo"
    repo.host = "localhost:3000"
    repo.scheme = "http"
    repo.url = "http://localhost:3000/test-user/test-repo"
    repo.exists.return_value = True
    return repo


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

    remote_repo = RemoteRepo(remote_repo.identifier, host=TEST_HOST, scheme=TEST_SCHEME)

    yield local_repo, remote_repo


# Additional common fixtures for testing infrastructure

@pytest.fixture
def temp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_config():
    """Provide a mock configuration object for testing."""
    config = MagicMock()
    config.get = Mock(side_effect=lambda key, default=None: {
        "api_key": "test_api_key_123",
        "host": "localhost:3000",
        "scheme": "http",
        "timeout": 30,
        "max_retries": 3
    }.get(key, default))
    return config


@pytest.fixture
def sample_dataframe():
    """Create a sample pandas DataFrame for testing."""
    import pandas as pd
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'score': [85.5, 92.3, 78.9, 95.1, 88.7],
        'active': [True, False, True, True, False]
    }
    return pd.DataFrame(data)


@pytest.fixture
def sample_json_data():
    """Provide sample JSON data for testing."""
    return {
        "version": "1.0.0",
        "metadata": {
            "created_at": "2024-01-01T00:00:00Z",
            "author": "test_user"
        },
        "data": [
            {"id": 1, "value": "test1"},
            {"id": 2, "value": "test2"}
        ]
    }


@pytest.fixture
def mock_http_response():
    """Create a mock HTTP response for testing API calls."""
    response = Mock()
    response.status_code = 200
    response.json.return_value = {"status": "success", "data": []}
    response.text = '{"status": "success", "data": []}'
    response.headers = {"Content-Type": "application/json"}
    return response


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create a sample CSV file for testing."""
    csv_path = os.path.join(temp_dir, "sample.csv")
    content = """id,name,value
1,Item1,100
2,Item2,200
3,Item3,300"""
    with open(csv_path, 'w') as f:
        f.write(content)
    yield csv_path


@pytest.fixture
def sample_parquet_file(temp_dir):
    """Create a sample Parquet file for testing."""
    import pandas as pd
    import pyarrow.parquet as pq
    
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    df = pd.DataFrame({
        'id': range(100),
        'value': [f'value_{i}' for i in range(100)]
    })
    df.to_parquet(parquet_path)
    yield parquet_path


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up any test files created during tests."""
    created_files = []
    
    def register_file(filepath):
        created_files.append(filepath)
    
    request.addfinalizer(lambda: [
        os.remove(f) for f in created_files if os.path.exists(f)
    ])
    
    return register_file


@pytest.fixture
def mock_remote_repo():
    """Create a mock RemoteRepo for testing without actual network calls."""
    repo = Mock(spec=RemoteRepo)
    repo.identifier = "test-user/test-repo"
    repo.host = "localhost:3000"
    repo.scheme = "http"
    repo.url = "http://localhost:3000/test-user/test-repo"
    repo.exists.return_value = True
    return repo
