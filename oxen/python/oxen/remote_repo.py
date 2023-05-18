import json
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
        revision: str
            The branch name or commit id to checkout. Defaults to `main`
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

    def checkout(self, revision: str, create=False):
        """
        Switches the remote repo to the specified revision.

        Parameters
        ----------
        revision : str
            The name of the branch or commit id to checkout.
        create : bool
            Whether to create a new branch if it doesn't exist. Default: False
        """
        if create:
            return self._repo.create_branch(revision)

        self._repo.checkout(revision)

    def download(self, remote_path: str, local_path: str | None):
        """
        Download a file or directory from the remote repo.

        Parameters
        ----------
        remote_path : str
            The path to the remote file
        local_path : str | None
            The path to the local file. If None, will download to
            the same path as remote_path
        """
        if local_path is None:
            local_path = remote_path
        self._repo.download(remote_path, local_path)

    def add(self, local_path: str, remote_directory: str = ""):
        """
        Stage a file to the remote staging environment

        Parameters
        ----------
        path: str
            The path to the local file to be staged
        remote_directory: str
            The path in the remote repo where the file will be added
        """
        self._repo.add(remote_directory, local_path)

    def remove(self, path: str):
        """
        Unstage a file from the remote staging environment

        Parameters
        ----------
        path: str
            The path to the file on remote to be removed from staging
        """
        self._repo.remove(path)

    def status(self, path: str = ""):
        """
        Get the status of the remote repo. Returns a StagedData object.

        Parameters
        ----------
        path: str
            The directory or file path on the remote that
            will be checked for modifications
        """
        return self._repo.status(path)

    def commit(self, message: str):
        """
        Commit the staged data in the remote repo with a message.

        Parameters
        ----------
        message : str
            The commit message.
        """
        self._repo.commit(message)

    def log(self):
        """
        Get the commit history for a remote repo
        """
        return self._repo.log()

    def list_branches(self):
        """
        List all branches for a remote repo
        """
        return self._repo.list_branches()

    def add_df_row(self, path: str, row: dict):
        """
        Adds a row to the dataframe at the specified path on the remote repo

        Parameters
        ----------
        path: str
            Path to the dataframe on the remote repo
        row: dict
            A dictionary representing the row to be added to the dataframe,
            where keys are column names and values are the values to be inserted.
            Schema must exactly match DF on remote repo.
        """
        data = json.dumps(row)
        return self._repo.add_df_row(path, data)

    def get_branch(self, branch: str):
        """
        Return a branch by name on this repo, if exists

        Parameters
        ----------
        branch: str
            The name of the branch to return
        """
        return self._repo.get_branch(branch)

    def create_branch(self, new_branch: str):
        """
        Return a branch by name on this repo,
        creating it from the currently checked out branch if it doesn't exist

        Parameters
        ----------
        new_branch: str
            The name to assign to the created branch
        """
        return self._repo.create_branch(new_branch)
