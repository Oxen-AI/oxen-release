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
