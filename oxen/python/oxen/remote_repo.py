from oxen import PyRemoteRepo


class RemoteRepo:
    """
    Remote repository object that allows you to interact with a remote oxen repository.
    """

    def __init__(self, path: str, host: str = "hub.oxen.ai", revision: str = "main"):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        path : str
            Name of the repository in the format `namespace/repo_name`.
            For example `ox/chatbot`
        host : str
            The host to connect to. Defaults to `hub.oxen.ai`
        revision : str
            The branch name or commit id to download from
        """
        self._repo = PyRemoteRepo(path, host, revision)

    def __repr__(self):
        return f"RemoteRepo({self._repo.url()})"

    @property
    def namespace(self) -> str:
        """
        The namespace for the repo.
        """
        return self._repo.namespace()

    @property
    def name(self) -> str:
        """
        The name of the repo.
        """
        return self._repo.name()

    @property
    def url(self) -> str:
        """
        The remote url for the repo.
        """
        return self._repo.url()

    @property
    def revision(self) -> str:
        """
        The branch or commit id for the repo
        """
        return self._repo.revision()

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
        self, remote_path: str, local_path: str, revision: str | None = "main"
    ):
        """
        Download a file or directory from the remote repo.

        Parameters
        ----------
        remote_path : str
            The path to the remote file
        local_path : str
            The path to the local file.
        revision : str | None
            The branch name or commit id to download from
        """
        if revision is None:
            revision = self.revision
        self._repo.download(remote_path, local_path, revision)
        
    def add(self, path: str, branch_name: str = "main", directory_name: str = ""):
        """
        Stage a file to be committed.
        """
        self._repo.add(path, branch_name, directory_name)

    def get_branch(self, branch_name: str):
        """
        Return a branch at the specified name    
        """
        self._repo.get_branch(branch_name)
