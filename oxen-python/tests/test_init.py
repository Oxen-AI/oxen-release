import os

from oxen import LocalRepo


def test_init(empty_local_dir):
    # Test that we make the oxen hidden dir when we init()
    oxen_hidden_dir = os.path.join(empty_local_dir, ".oxen")
    assert not os.path.exists(oxen_hidden_dir)

    repo = LocalRepo(empty_local_dir)
    repo.init()

    assert os.path.exists(oxen_hidden_dir)
