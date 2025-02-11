import os
import fsspec
import pandas as pd

from oxen import RemoteRepo


def test_fsspec_read_file(
    celeba_remote_repo_one_image_pushed: RemoteRepo, shared_datadir
):
    _, remote_repo = celeba_remote_repo_one_image_pushed
    # images/1.jpg pushed in setup
    namespace = remote_repo._repo.namespace()
    repo_name = remote_repo._repo.name()
    host = remote_repo._repo.host
    scheme = remote_repo._repo.scheme
    fs = fsspec.filesystem(
        "oxen", namespace=namespace, repo=repo_name, host=host, scheme=scheme
    )
    with fs.open("images/1.jpg", mode="rb") as f:
        remote_image_file = f.read()

    with open(os.path.join(shared_datadir, "CelebA/images/1.jpg"), "rb") as f:
        assert remote_image_file == f.read()


def test_fsspec_write_file(
    chat_bot_remote_repo_fully_pushed: RemoteRepo, shared_datadir
):
    local_repo, remote_repo = chat_bot_remote_repo_fully_pushed
    # prompt.txt pushed in setup
    namespace = remote_repo._repo.namespace()
    repo_name = remote_repo._repo.name()
    host = remote_repo._repo.host
    scheme = remote_repo._repo.scheme
    fs = fsspec.filesystem(
        "oxen", namespace=namespace, repo=repo_name, host=host, scheme=scheme
    )
    with fs.open("prompt.txt", mode="wb") as f:
        f.commit_message = "Updated prompt"
        f.write("This is an updated prompt!")

    # should have 2 commits now
    assert len(remote_repo.log()) == 2

    local_repo.pull()
    with open(os.path.join(local_repo.path, "prompt.txt"), "r") as f:
        assert f.read() == "This is an updated prompt!"


def test_fsspec_write_file_with_pandas(
    chat_bot_remote_repo_fully_pushed: RemoteRepo, shared_datadir
):
    local_repo, remote_repo = chat_bot_remote_repo_fully_pushed
    # examples.tsv pushed in setup
    namespace = remote_repo._repo.namespace()
    repo_name = remote_repo._repo.name()
    host = remote_repo._repo.host
    scheme = remote_repo._repo.scheme
    fs = fsspec.filesystem(
        "oxen", namespace=namespace, repo=repo_name, host=host, scheme=scheme
    )
    # Read TSV
    with fs.open("examples.tsv") as f:
        df = pd.read_csv(f, delimiter="\t")

    assert df.shape == (6, 2)

    # Write as parquet
    with fs.open("examples.parquet", mode="wb") as f:
        df.to_parquet(f)

    local_repo.pull()
    with open(os.path.join(local_repo.path, "examples.parquet"), "rb") as f:
        df_new = pd.read_parquet(f)
        assert df_new.equals(df)
