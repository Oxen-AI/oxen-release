from oxen import PyDataset

import os
import polars as pl
from typing import Sequence, Union
from pathlib import Path
from typing import Optional


def load_dataset(repo, paths: Union[str, Sequence[str]], features=None):
    """
    Load a dataset from a repo.

    Parameters
    ----------
    repo : Repo
        The oxen repository you are loading data from
        can be a local or a remote repo
    features : Features | None
        The features of the dataset, columns, dtypes, etc.
    paths : str | Sequence[str]
        The paths to the data files needed to load the dataset
    """
    dataset = Dataset(repo, paths, features)
    return dataset


class Dataset:
    """
    Dataset object constructs a dataset from a remote or local repo.
    It can be used to load data into a dataloader.
    """

    # TODO: allow remote or local repos
    def __init__(
        self,
        repo,
        paths: Union[str, Sequence[str]],
        features=None,
        cache_dir: str = None,
        download: bool = False,
    ):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        repo : Repo
            The oxen repository you are loading data from
            can be a local or a remote repo
        features : Features | None
            The features of the dataset, columns, dtypes, etc.
        paths : str | Sequence[str]
            The paths to the data files needed to load the dataset
        cache_dir : str
            The directory to download/cache the data in.
        download : bool
            Whether to download the data or not.
        """
        self._repo = repo
        if cache_dir is None:
            self._cache_dir = Dataset.default_cache_dir(repo)
        else:
            self._cache_dir = cache_dir

        if isinstance(paths, str):
            self._paths = [paths]
        else:
            self._paths = paths

        self._data_frames = []
        self._features = features
        self.downloaded = False

        if download:
            self.download_all()
        else:
            self.sizes = [self._repo.get_df_size(path) for path in self._paths]
            width = sum([size[0] for size in self.sizes])
            height = sum([size[1] for size in self.sizes])
            self.size = width, height

    # For iterating over the dataset
    def __len__(self):
        if self.downloaded:
            return sum([df.height for df in self._data_frames])
        else:
            return self.size[1]

    # For iterating over the dataset
    def __getitem__(self, idx):
        # FInd which dataframes we are in
        df_idx, df_offset = self._get_df_offsets(idx)

        # Offset is the row we are at in the data frame
        remainder = idx - df_offset

        ret_features = []
        if self.downloaded:
            # extract the features from the data frame
            for feature in self.features:
                df = self._data_frames[df_idx]
                val = df[feature.name][remainder]
                ret_features.append(val)
        else:
            # Grab from the API
            row = self._get_df_row(idx)
            ret_features.append(row)

        return ret_features

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]

    def __repr__(self):
        return f"Dataset({self._repo}, {self._paths})"

    def __str__(self):
        return f"Dataset({self._repo}, {self._paths})"

    def _get_df_row(self, idx):
        df_idx, df_offset = self._get_df_offsets(idx)
        path = self._paths[df_offset]
        return self._repo.get_df_row(path, df_idx)

    def _get_df_offsets(self, idx):
        # iterate over data frames to find the right one to index
        df_offset = 0  # which row we are at
        df_idx = 0  # which dataframe we are on

        # create a running sum of the heights of the data frames
        heights = [size[1] for size in self.sizes]
        summed_heights = [sum(heights[: i + 1]) for i in range(len(heights))]

        # find which data frame we are in
        for i, height in enumerate(summed_heights):
            if idx > height:
                df_offset = i

        df_idx = idx
        if df_idx > summed_heights[df_offset]:
            df_idx = idx - summed_heights[df_offset]

        return df_idx, df_offset

    @staticmethod
    def default_cache_dir(repo) -> str:
        # ~/.oxen/.cache/<namespace>/<repo>/<revision>
        cache_dir = os.path.join(Path.home(), ".oxen")
        cache_dir = os.path.join(cache_dir, ".cache")
        cache_dir = os.path.join(cache_dir, repo.namespace)
        cache_dir = os.path.join(cache_dir, repo.name)
        cache_dir = os.path.join(cache_dir, repo.revision)
        return cache_dir

    def _cache_download_path(self, base_dir: Optional[str]) -> str:
        """
        Returns the path to the file given the base dir or defaults to
        the cache directory.
        """
        if base_dir is None:
            return self._cache_dir
        return base_dir

    def df(self, path: str, base_dir: str = None) -> pl.DataFrame:
        """
        Returns a dataframe of the data from the repo, fully loaded into memory.
        Only works if dataset has been downloaded to disk.

        Parameters
        ----------
        path : str
            Paths to the data frames you want to load.
        base_dir : str | None
            The base directory to download the data to.
        """
        # TODO:
        #  * handle streaming into memory without downloading to disk

        output_path = self.download(path, base_dir)
        df = PyDataset.df(output_path)
        return df

    def download(self, remote_path: str, base_dir: str = None) -> str:
        """
        Returns the path to the downloaded file.

        Parameters
        ----------
        path : str
            Paths to the file you want to load.
        base_dir : str | None
            The base directory to download the data to.
        """
        # Download file to cache path if it does not already exist
        # Cache path has the revision, so if the revision changes, it will
        # download the new data
        base_dir = self._cache_download_path(base_dir)
        output_path = os.path.join(base_dir, remote_path)
        parent = os.path.dirname(output_path)
        if not os.path.exists(parent):
            os.makedirs(parent, exist_ok=True)
        if not os.path.exists(output_path):
            self._repo.download(remote_path, parent)
        self.downloaded = True
        return output_path

    def download_all(self):
        """
        Download the data from the repo to disk
        """
        for path in self._paths:
            print(f"Downloading {path}...")

            path = self.download(path, self._cache_dir)
