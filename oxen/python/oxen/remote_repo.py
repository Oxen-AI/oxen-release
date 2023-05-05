from oxen import PyRemoteRepo


class RemoteRepo:
    """
    Remote repository object that allows you to interact with a remote oxen repository.
    """

    def __init__(self, path: str, host: str = "hub.oxen.ai"):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        path : str
            Name of the repository in the format `namespace/repo_name`.
            For example `ox/chatbot`
        """
        self._repo = PyRemoteRepo(path, host)

    def __repr__(self):
        return f"RemoteRepo({self._repo.url()})"

    @property
    def url(self) -> str:
        """
        Returns the remote url for the repo.
        """
        return self._repo.url()

    def create(self):
        """
        Will create the repo on the remote server.
        """
        self._repo.create()

    def exists(self) -> bool:
        """
        Checks if this remote repo exists on the server.
        """
        return self._repo.exists()

    def delete(self):
        """
        Delete this remote repo from the server.
        """
        self._repo.delete()

    def download(
        self, remote_path: str, local_path: str | None, committish: str = "main"
    ):
        """
        Download a file or directory from the remote repo.

        Parameters
        ----------
        remote_path : str
            The path to the remote file
        local_path : str | None
            The path to the local file. If None, will download to
            the same path as remote_path
        committish : str
            The branch name or commit id to download from
        """
        self._repo.download(remote_path, local_path, committish)
