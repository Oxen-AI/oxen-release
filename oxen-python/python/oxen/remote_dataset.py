
from oxen.remote_repo import RemoteRepo
from .oxen import PyRemoteDataset
import json
from typing import List

class RemoteDataset:
    """
    The RemoteDataset class allows you to perform CRUD operations on a data frame that is stored on a remote Oxen Server.
    """

    def __init__(self, repo: RemoteRepo, filename: str):
        self.repo = repo
        self.filename = filename
        self.dataset = PyRemoteDataset(repo._repo, filename)

    def __repr__(self):
        return f"RemoteDataset(repo={self.repo}, filename={self.filename})"
    
    def size(self) -> (int, int):
        """
        Get the size of the dataset. (rows, columns)
        """
        return self.dataset.size()

    def list(self) -> List[dict]:
        """
        List the data within the dataset.
        """
        results = self.dataset.list()
        # convert string to dict
        # this is not the most efficient but gets it working
        data = json.loads(results)
        
        # TODO: why do we use periods vs underscores...?
        # filter out .oxen.diff.hash and .oxen.diff.status and _oxen_row_id
        # from each element in the list of dicts
        filter_keys = [".oxen.diff.hash", ".oxen.diff.status", "_oxen_row_id"]
        data = [{k: v for k, v in d.items() if k not in filter_keys} for d in data]
        return data

    def insert_one(self, data: dict):
        """
        Insert a single row of data into the dataset.
        """
        # convert dict to json string
        # this is not the most efficient but gets it working
        data = json.dumps(data)
        return self.dataset.insert_one(data)

    def get_by_id(self, id: str):
        """
        Get a single row of data by id.
        """
        return self.dataset.get_by_id(id)

def index_dataset(repo: RemoteRepo, filename: str) -> RemoteDataset:
    """
    Index an existing file on a remote Oxen Server.
    """
    pass