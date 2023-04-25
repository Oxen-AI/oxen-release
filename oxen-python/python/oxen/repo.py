
import oxen

class Repo:
    """
    Local repository object that allows you to interact with your local oxen repo.
    
    Parameters
    ----------
    path : str
        Path to the main working directory of your oxen repo
    """
    def __init__(self, path: str):
        self._repo = oxen.PyRepo(path)
    
    def init(self):
        """
        Initializes a new oxen repository at the path specified in the constructor.
        """
        self._repo.init()

    def add(self, path: str):
        """
        Stage a file or directory to be committed.
        """
        self._repo.add(path)