from oxen import PyRepo


class Repo:
    """
    Local repository object that allows you to interact with your local oxen repo.
    """

    def __init__(self, path: str):
        """
        Create a new Repo object. Use .init() to initialize a new oxen repository,
        or pass the path to an existing one.

        Parameters
        ----------
        path : str
            Path to the main working directory of your oxen repo.
        """
        self._repo = PyRepo(path)

    @property
    def path(self):
        """
        Returns the path to the repo.
        """
        return self._repo.path()

    def init(self):
        """
        Initializes a new oxen repository at the path specified in the constructor.
        Will create a .oxen folder to store all the versions and metadata.
        """
        self._repo.init()

    def add(self, path: str):
        """
        Stage a file or directory to be committed.
        """
        self._repo.add(path)

    def status(self):
        """
        Check the status of the repo. Returns a StagedData object.
        """
        return self._repo.status()

    def commit(self, message: str):
        """
        Commit the staged data in a repo with a message.

        Parameters
        ----------
        message : str
            The commit message.
        """
        return self._repo.commit(message)

    def log(self):
        """
        Get the commit history for a repo.
        """
        return self._repo.log()

    def set_remote(self, name: str, url: str):
        """
        Map a name to a remote url.

        Parameters
        ----------
        name : str
            The name of the remote. Ex) origin
        url : str
            The url you want to map the name to. Ex) https://hub.oxen.ai/ox/chatbot
        """
        self._repo.set_remote(name, url)

    def push(self, remote_name: str = "origin", branch: str = "main"):
        """
        Push data to a remote repo from a local repo.

        Parameters
        ----------
        remote_name : str
            The name of the remote to push to.
        branch : str
            The name of the branch to push to.
        """
        return self._repo.push(remote_name, branch)
