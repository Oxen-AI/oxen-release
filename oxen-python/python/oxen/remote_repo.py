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

    def add(self, local_path: str, remote_directory: str = "", revision: str = "main"):
        """
        Stage a file to the remote staging environment

        Parameters
        ----------
        path: str
            The path to the local file to be staged
        remote_directory: str
            The path in the remote repo where the file will be added
        revision: str
            The branch name or commit id to stage the commit on
        """
        self._repo.add(revision, remote_directory, local_path)

    def remove(self, path: str, branch: str = "main"):
        """
        Unstage a file from the remote staging environment

        Parameters
        ----------
        path: str
            The path to the file on remote to be removed from staging
        branch: str
            The branch name on which to unstage this file
        """
        self._repo.remove(branch, path)

    def status(self, revision: str = "main", path: str = ""):
        """
        Get the status of the remote repo. Returns a StagedData object.

        Parameters
        ----------
        revision: str
            The branch name or commit id to check the status of
        path: str
            The directory or file path on the remote that 
            will be checked for modifications
        """
        return self._repo.status(revision, path)

    def commit(self, message: str, branch: str = "main"):
        """
        Commit the staged data in the remote repo with a message.

        Parameters
        ----------
        message : str
            The commit message.
        branch:
            The remote branch name to commit to 
        """
        self._repo.commit(branch, message)

    def log(self, revision: str = "main"):
        """
        Get the commit history for a remote repo

        Parameters
        ----------
        revision: str
            The branch name or commit id to get history from
        """
        return self._repo.log(revision)

    def list_branches(self):
        """
        List all branches for a remote repo
        """
        return self._repo.list_branches()

    def get_branch(self, branch: str):
        """
        Return a branch by name if it exists

        Parameters
        ----------
        branch: str
            The branch name to get   
        """
        return self._repo.get_branch(branch)

    def create_branch(self, new_branch: str, from_branch: str = "main"):
        """
        Return a branch by name on this repo, 
        creating it from a specified existing branch if it doesn't exist

        Parameters
        ----------
        new_branch: str
            The name to assign to the created branch 
        from_branch: str
            The name of the branch to branch the new branch off of
        """
        return self._repo.create_branch(new_branch, from_branch)
