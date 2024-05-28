from oxen.remote_repo import RemoteRepo
from .oxen import PyRemoteDataset
from .oxen import remote_dataset
import json
from typing import List


def index_dataset(repo: RemoteRepo, filename: str):
    """
    Index an existing file on a remote Oxen Server.
    """
    remote_dataset.index_dataset(repo._repo, filename)


class RemoteDataset:
    """
    The RemoteDataset class allows you to perform CRUD operations on a data frame that is stored on a remote Oxen Server.
    """

    def __init__(self, repo: RemoteRepo, filename: str):
        self.repo = repo
        self.filename = filename
        self.dataset = PyRemoteDataset(repo._repo, filename)
        # TODO: why do we use periods vs underscores...? Fix this in the Rust code.
        self.filter_keys = [".oxen.diff.hash", ".oxen.diff.status", "_oxen_row_id"]

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
        data = self._filter_keys_arr(data)
        return data

    def insert_row(self, data: dict):
        """
        Insert a single row of data into the dataset.
        """
        # convert dict to json string
        # this is not the most efficient but gets it working
        data = json.dumps(data)
        return self.dataset.insert_row(data)

    def get_row_by_id(self, id: str):
        """
        Get a single row of data by id.
        """
        data = self.dataset.get_row_by_id(id)
        # convert string to dict
        # this is not the most efficient but gets it working
        data = json.loads(data)
        # filter out .oxen.diff.hash and .oxen.diff.status and _oxen_row_id
        data = self._filter_keys_arr(data)

        if len(data) == 0:
            return None
        return data[0]

    def update_row(self, id: str, data: dict):
        """
        Update a single row of data by id.
        """
        data = json.dumps(data)
        result = self.dataset.update_row(id, data)
        result = json.loads(result)
        result = self._filter_keys_arr(result)
        return result

    def delete_row(self, id: str):
        """
        Delete a single row of data by id.
        """
        return self.dataset.delete_row(id)

    def _filter_keys(self, data: dict):
        """
        Filter out the keys that are not needed in the dataset.
        """
        # TODO: why do we use periods vs underscores...?
        # filter out .oxen.diff.hash and .oxen.diff.status and _oxen_row_id
        # from each element in the list of dicts
        return {k: v for k, v in data.items() if k not in self.filter_keys}

    def _filter_keys_arr(self, data: List[dict]):
        """
        Filter out the keys that are not needed in the dataset.
        """
        return [self._filter_keys(d) for d in data]
