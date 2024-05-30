from oxen.remote_repo import RemoteRepo
from .oxen import PyRemoteDataset
from .oxen import remote_dataset
import json
from typing import List


def index_dataset(repo: RemoteRepo, filename: str):
    """
    Index an existing file on a remote Oxen Server.
    
    Args:
        repo: `RemoteRepo`
            The repository to index the dataset in.
        filename: `str`
            The name of the file to index.
    """
    remote_dataset.index_dataset(repo._repo, filename)


class RemoteDataset:
    """
    The RemoteDataset class allows you to perform CRUD operations on a data frame that is stored on a remote Oxen Server.
    """

    def __init__(self, repo: RemoteRepo, filename: str):
        """
        Initialize the RemoteDataset class. Will throw an error if the dataset does not exist or is not indexed.

        Args:
            repo: `RemoteRepo`
                The repository to index the dataset in.
            filename: `str`
                The name of the file to index.
        """
        self.repo = repo
        self.filename = filename
        # this will return an error if the dataset does not exist or is not indexed
        self.dataset = PyRemoteDataset(repo._repo, filename)
        # TODO: why do we use periods vs underscores...? Fix this in the Rust code.
        self.filter_keys = [".oxen.diff.hash", ".oxen.diff.status", "_oxen_row_id"]

    def __repr__(self):
        return f"RemoteDataset(repo={self.repo}, filename={self.filename})"

    def size(self) -> (int, int):
        """
        Get the size of the dataset. Returns a tuple of (rows, columns)
        """
        return self.dataset.size()
    
    def page_size(self) -> int:
        """
        Get the page size of the dataset for pagination in list() command.

        Returns:
            The page size of the dataset.
        """
        return self.dataset.page_size()
    
    def total_pages(self) -> int:
        """
        Get the total number of pages in the dataset for pagination in list() command.

        Returns:
            The total number of pages in the dataset.
        """
        return self.dataset.total_pages()

    def list_page(self, page_num: int = 1) -> List[dict]:
        """
        List the rows within the dataset.

        Args:
            page_num: `int`
                The page number of the dataset to list. We default to page size of 100 for now.

        Returns:
            A list of rows from the dataset.
        """
        results = self.dataset.list(page_num)
        # convert string to dict
        # this is not the most efficient but gets it working
        data = json.loads(results)
        data = self._filter_keys_arr(data)
        return data

    def insert_row(self, data: dict):
        """
        Insert a single row of data into the dataset.
        
        Args:
            data: `dict`
                A dictionary representing a single row of data. 
                The keys must match a subset of the columns in the dataset. 
                If a column is not present in the dictionary, 
                it will be set to an empty value.

        Returns:
            The id of the row that was inserted.
        """
        # convert dict to json string
        # this is not the most efficient but gets it working
        data = json.dumps(data)
        return self.dataset.insert_row(data)

    def get_row_by_id(self, id: str):
        """
        Get a single row of data by id.
        
        Args:
            id: `str`
                The id of the row to get.

        Returns:
            A dictionary representing the row.
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

        Args:
            id: `str`
                The id of the row to update.
            data: `dict`
                A dictionary representing a single row of data. 
                The keys must match a subset of the columns in the dataset. 
                If a column is not present in the dictionary, 
                it will be set to an empty value.

        Returns:
            The updated row as a dictionary.
        """
        data = json.dumps(data)
        result = self.dataset.update_row(id, data)
        result = json.loads(result)
        result = self._filter_keys_arr(result)
        return result

    def delete_row(self, id: str):
        """
        Delete a single row of data by id.
        
        Args:
            id: `str`
                The id of the row to delete.
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
