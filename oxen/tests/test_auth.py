import oxen
import os
import toml


def test_create_user(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    oxen.auth.create_user_config("test user", "test_user@test.co", path)
    config = toml.load(path)
    assert config["name"] == "test user"
    assert config["email"] == "test_user@test.co"


def test_add_host(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    oxen.auth.create_user_config("test user", "test_user@test.co", path)
    oxen.auth.add_host_auth("test_host", "abcdefghijklmnop", path)
    config = toml.load(path)
    assert config["host_configs"][0]["host"] == "test_host"
    assert config["host_configs"][0]["auth_token"] == "abcdefghijklmnop"


def test_add_three_hosts(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    oxen.auth.create_user_config("test user", "test_user@test.co", path)
    oxen.auth.add_host_auth("one", "abc", path)
    oxen.auth.add_host_auth("two", "def", path)
    oxen.auth.add_host_auth("three", "hij", path)
    config = toml.load(path)
    assert set([config["host_configs"][i]["host"] for i in range(3)]) == set(
        ["one", "two", "three"]
    )
    assert set([config["host_configs"][i]["auth_token"] for i in range(3)]) == set(
        ["abc", "def", "hij"]
    )


def test_double_create_should_update(shared_datadir):
    path = os.path.join(shared_datadir, "config", "user_config.toml")
    oxen.auth.create_user_config("test user", "test_user@test.co", path)
    oxen.auth.create_user_config("new", "new@s.co", path)
    config = toml.load(path)
    assert config["name"] == "new"
    assert config["email"] == "new@s.co"
