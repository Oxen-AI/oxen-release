# Read up on data, dataset, and dataloader APIs in
#   * pytorch
#   * huggingface
#   * tensorflow
#
# Others to consider:
#   * keras
#   * fastai
#   * pytorch-lightning
#   * jax
#   * sklearn
#   * pandas
#   * numpy
#   * scipy

# Start going through tutorials for each of the above,
# and write example code for each of the above
# fit into a boilerplate similar to the ImageClassificationBoilerplate and
# be sure that the Oxen data APIs can easily slot in,
# with swapping out the model, training loop, eval loop, etc.

# Design APIs to be easy to fetch random examples from remote data frames to serve up
# to our "gradio chatbot labeling tool" (or any other labeling tool)

# Design underlying Oxen APIs to easily conform to the above APIs
#
# Keith: How impossible would building a Python library that exposes a read-only
# dataframe on top of an oxen repo be?

# Read-only dataframe API:
# 0) download a file to disk from repo (easy)
# 1) load a dataframe into python, from downloaded file (easy)
# 2) slice and stream a dataframe in current format (easy, already have apis,
#    potentially slow to slice big CSVs)
# 3) slice and stream a dataframe after converting for apache arrow for you on disk
#    (way more efficient, can convert on disk, do we do out of the box?)

from oxen import PyDataset

import os
import polars as pl
from typing import Sequence, Union
from pathlib import Path


class Dataset:
    """
    Dataset object constructs a dataset from a remote or local repo.
    It can be used to load data into a dataloader.
    """

    # TODO: allow remote or local repos
    def __init__(self, repo, cache_dir: str = None):
        """
        Create a new RemoteRepo object to interact with.

        Parameters
        ----------
        repo : Repo
            The oxen repository you are loading data from
            can be a local or a remote repo
        cache_dir : str
            The directory to download/cache the data in.
        """
        self._repo = repo
        if cache_dir is None:
            self._cache_dir = Dataset.default_cache_dir(repo)
        else:
            self._cache_dir = cache_dir

        self._data_files = []
        self._data_frames = []

    @staticmethod
    def default_cache_dir(repo) -> str:
        # ~/.oxen/data/<namespace>/<repo>/<revision>
        cache_dir = os.path.join(Path.home(), ".oxen")
        cache_dir = os.path.join(cache_dir, "data")
        cache_dir = os.path.join(cache_dir, repo.namespace)
        cache_dir = os.path.join(cache_dir, repo.name)
        cache_dir = os.path.join(cache_dir, repo.revision)
        return cache_dir

    def _cache_path(self, path: str) -> str:
        """
        Returns the path to the file in the cache directory.
        """
        return os.path.join(self._cache_dir, path)

    def _df_download_path(self, path, base_dir: str | None) -> str:
        """
        Returns the path to the file given the base dir or defaults to
        the cache directory.
        """
        if base_dir is None:
            return self._cache_path(path)
        return os.path.join(base_dir, path)

    # TODO: optionally download data
    def df(self, path: str, base_dir: str = None) -> pl.DataFrame:
        """
        Returns a dataframe of the data from the repo.
            Parameters
        ----------
        path : str
            Paths to the data frames you want to load.
        base_dir : str | None
            The base directory to download the data to.
        """
        # TODO:
        #  * handle streaming into memory without downloading to disk

        # Download file to cache path if it does not already exist
        # Cache path has the revision, so if the revision changes, it will
        # download the new data
        output_path = self._df_download_path(path, base_dir)
        if not os.path.exists(output_path):
            os.makedirs(os.path.dirname(output_path))
            self._repo.download(path, output_path)

        df = PyDataset.df(output_path)
        return df

    def load(
        self, data: Union[str, Sequence[str]], download: bool = True
    ) -> pl.DataFrame:
        """
        Load the data from the repo into a dataframe.

        Parameters
        ----------
        data : str | Sequence[str]
            Path or paths to the data frames you want to load.

            If you pass a string, it will pull the data from the  at that path.
            If you pass a sequence, it will
        """
        if isinstance(data, str):
            self._data_files = [data]
        else:
            self._data_files = data

        if download:
            for data_file in self._data_files:
                print(f"Downloading {data_file}...")
                df = self.df(data_file, self._cache_dir)
                self._data_frames.append(df)
