from oxen.local_repo import LocalRepo


def init(
    path: str = "./",
):
    """
    Initialize a [LocalRepo](/python-api/local_repo) at the given path.

    Args:
        path: `str`
            The path to initialize the repo at.
     Returns:
        [LocalRepo](/python-api/local_repo)
            A LocalRepo object that can be used to interact with the repo.
    """
    # Init Repo
    local_repo = LocalRepo(path)
    return local_repo.init()
